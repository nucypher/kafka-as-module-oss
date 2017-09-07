package com.nucypher.kafka.proxy;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
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
                    Send send;
                    if (requestOrResponse.isRequest) {
                        send = recreateProduceRequest(requestOrResponse.destination,
                                requestOrResponse.requestHeader,
                                requestOrResponse.produceRequest,
                                requestOrResponse.serializer);
                    } else {
                        send = recreateFetchResponse(requestOrResponse.destination,
                                requestOrResponse.requestHeader,
                                requestOrResponse.fetchResponse,
                                requestOrResponse.deserializer);
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

    private Send recreateProduceRequest(String destination,
                                        RequestHeader header,
                                        ProduceRequest produceRequest,
                                        Serializer<byte[]> serializer) {
        Map<TopicPartition, MemoryRecords> produceRecordsByPartition =
                produceRequest.partitionRecordsOrFail();
        for (Map.Entry<TopicPartition, MemoryRecords> entry :
                produceRecordsByPartition.entrySet()) {
            MemoryRecords records = entry.getValue();
            if (records.records().iterator().hasNext()) {
                MemoryRecords newRecords = transformRecords(
                        entry.getKey().topic(), records, ENCRYPT_FACTOR, serializer, null);
                entry.setValue(newRecords);
            }
        }

        ProduceRequest newProduceRequest = new ProduceRequest.Builder(
                RecordBatch.CURRENT_MAGIC_VALUE,
                produceRequest.acks(),
                produceRequest.timeout(),
                produceRecordsByPartition).build(header.apiVersion());
        return newProduceRequest.toSend(destination, header);
    }

    private Send recreateFetchResponse(String destination,
                                       RequestHeader header,
                                       FetchResponse response,
                                       Deserializer<byte[]> deserializer) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> data = response.responseData();
        for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : data.entrySet()) {
            FetchResponse.PartitionData partitionData = entry.getValue();
            Records records = partitionData.records;
            if (records.sizeInBytes() > 0) {
                Records newRecords = transformRecords(
                        entry.getKey().topic(), records, DECRYPT_FACTOR, null, deserializer);
                entry.setValue(new FetchResponse.PartitionData(
                        partitionData.error,
                        partitionData.highWatermark,
                        partitionData.lastStableOffset,
                        partitionData.logStartOffset,
                        partitionData.abortedTransactions,
                        newRecords));
            }
        }
        response = new FetchResponse(data, response.throttleTimeMs());
        return response.toSend(destination, header);
    }

    private MemoryRecords transformRecords(String topic,
                                           Records records,
                                           int sizeFactor,
                                           Serializer<byte[]> serializer,
                                           Deserializer<byte[]> deserializer) {
        int bufferSize = records.sizeInBytes() * sizeFactor;
        MemoryRecordsBuilder builder = null;
        for (RecordBatch recordBatch : records.batches()) {
            if (builder == null) {
                //assume that all batches has the same compression and others parameters
                builder = MemoryRecords.builder(
                        ByteBuffer.allocate(bufferSize),
                        recordBatch.compressionType(),
                        recordBatch.timestampType(),
                        recordBatch.baseOffset());
            }
            for (Record record : recordBatch) {
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
                builder.appendWithOffset(
                        record.offset(),
                        record.timestamp(),
                        key != null ? key.array() : null,
                        valueBytes);
            }
        }
        builder.closeForRecordAppends();
        return builder.build();
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
//        private final ResponseHeader responseHeader;
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
//            responseHeader = null;
            fetchResponse = null;
            deserializer = null;
        }

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
//                          ResponseHeader responseHeader,
                          FetchResponse fetchResponse,
                          Deserializer<byte[]> deserializer) {
            this.processor = processor;
            this.destination = destination;
//            this.responseHeader = responseHeader;
            this.fetchResponse = fetchResponse;
            this.deserializer = deserializer;
            this.requestHeader = requestHeader;
            isRequest = false;
            produceRequest = null;
            serializer = null;
        }
    }
}
