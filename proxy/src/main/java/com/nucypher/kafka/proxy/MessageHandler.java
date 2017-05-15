package com.nucypher.kafka.proxy;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseSend;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Produce requests and fetch responses handler
 */
public class MessageHandler extends Thread implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    private static final int ENCRYPT_FACTOR = 10; //TODO calculate
    private static final int DECRYPT_FACTOR = 1;

    private final int id;
    private BlockingQueue<RequestOrResponse> queue = new LinkedBlockingQueue<>();

    /**
     * @param id message handler id
     */
    public MessageHandler(int id) {
        this.id = id;
        setName("message-handler-" + id);
    }

    /**
     * Enqueue request or response for handling
     *
     * @param requestOrResponse request or response
     */
    public void enqueue(RequestOrResponse requestOrResponse) {
        queue.add(requestOrResponse);
    }

    @Override
    public void run() {
        LOGGER.info("Message handler {} was started", id);
        try {
            while (!isInterrupted()) {
                RequestOrResponse requestOrResponse = queue.take();
                try {
                    NetworkSend send;
                    if (requestOrResponse.isRequest) {
                        send = new RequestSend(requestOrResponse.destination,
                                requestOrResponse.requestHeader,
                                recreateProduceRequest(
                                        requestOrResponse.produceRequest,
                                        requestOrResponse.serializer));
                    } else {
                        send = new ResponseSend(requestOrResponse.destination,
                                requestOrResponse.responseHeader,
                                recreateFetchResponse(
                                        requestOrResponse.fetchResponse,
                                        requestOrResponse.deserializer));
                    }
                    requestOrResponse.processor.send(send);
                } catch (Exception e) {
                    LOGGER.error("Error while handling message", e);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.info("Message handler {} was interrupted", id);
        } finally {
            close();
        }
    }

    private Struct recreateProduceRequest(ProduceRequest produceRequest,
                                          Serializer<byte[]> serializer) {
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = produceRequest.partitionRecords();
        for (Map.Entry<TopicPartition, ByteBuffer> entry :
                produceRecordsByPartition.entrySet()) {
            MemoryRecords records = MemoryRecords.readableRecords(entry.getValue());
            if (records.iterator().hasNext()) {
                ByteBuffer buffer = getByteBuffer(
                        entry.getKey().topic(), records, ENCRYPT_FACTOR, serializer, null);
                entry.setValue(buffer);
            }
        }

        ProduceRequest newProduceRequest = new ProduceRequest(
                produceRequest.acks(), produceRequest.timeout(), produceRecordsByPartition);
        return newProduceRequest.toStruct();
    }

    private Struct recreateFetchResponse(FetchResponse response,
                                         Deserializer<byte[]> deserializer) {
        Map<TopicPartition, FetchResponse.PartitionData> data = response.responseData();
        for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : data.entrySet()) {
            FetchResponse.PartitionData partitionData = entry.getValue();
            MemoryRecords records = MemoryRecords.readableRecords(partitionData.recordSet);
            if (records.iterator().hasNext()) {
                ByteBuffer buffer = getByteBuffer(
                        entry.getKey().topic(), records, DECRYPT_FACTOR, null, deserializer);
                entry.setValue(new FetchResponse.PartitionData(
                        partitionData.errorCode, partitionData.highWatermark, buffer));
            }
        }
        response = new FetchResponse(data, response.getThrottleTime());
        return response.toStruct();
    }

    private ByteBuffer getByteBuffer(String topic,
                                     MemoryRecords records,
                                     int sizeFactor,
                                     Serializer<byte[]> serializer,
                                     Deserializer<byte[]> deserializer) {
        int bufferSize = records.sizeInBytes() * sizeFactor;
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        MemoryRecords memoryRecords = null;

        for (LogEntry logEntry : records) {
            Record record = logEntry.record();
            if (memoryRecords == null) {
                CompressionType compressionType = record.compressionType();
                memoryRecords = MemoryRecords.emptyRecords(buffer, compressionType);
            }

            ByteBuffer payload = record.value().duplicate();
            byte[] valueBytes = null;
            if (payload != null) {
                valueBytes = new byte[payload.remaining()];
                payload.get(valueBytes);
            }
            if (deserializer != null) {
                valueBytes = deserializer.deserialize(topic, valueBytes); //TODO if error need send response
            }
            if (serializer != null) {
                valueBytes = serializer.serialize(topic, valueBytes); //TODO if error need send response
            }
            ByteBuffer key = record.key();
            memoryRecords.append(
                    logEntry.offset(),
                    record.timestamp(),
                    key != null ? key.array() : null,
                    valueBytes);
        }
        memoryRecords.close();
        return memoryRecords.buffer();
    }

    @Override
    public void close() {
        if (!isInterrupted()) {
            interrupt();
        }
        LOGGER.info("Message handler {} was stopped", id);
    }

    /**
     * Request or response data holder
     */
    static class RequestOrResponse {
        private final Processor processor;
        private final String destination;
        private final RequestHeader requestHeader;
        private final ProduceRequest produceRequest;
        private final ResponseHeader responseHeader;
        private final FetchResponse fetchResponse;
        private final boolean isRequest;
        private final Serializer<byte[]> serializer;
        private final Deserializer<byte[]> deserializer;

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
                          ProduceRequest produceRequest,
                          Serializer<byte[]> serializer) {
            this.processor = processor;
            this.destination = destination;
            this.requestHeader = requestHeader;
            this.produceRequest = produceRequest;
            this.serializer = serializer;
            isRequest = true;
            responseHeader = null;
            fetchResponse = null;
            deserializer = null;
        }

        RequestOrResponse(Processor processor,
                          String destination,
                          ResponseHeader responseHeader,
                          FetchResponse fetchResponse,
                          Deserializer<byte[]> deserializer) {
            this.processor = processor;
            this.destination = destination;
            this.responseHeader = responseHeader;
            this.fetchResponse = fetchResponse;
            this.deserializer = deserializer;
            isRequest = false;
            requestHeader = null;
            produceRequest = null;
            serializer = null;
        }
    }
}
