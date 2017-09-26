package com.nucypher.kafka.proxy.handler;

import com.nucypher.kafka.clients.ReEncryptionHandler;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.proxy.Processor;
import com.nucypher.kafka.proxy.Utils;
import com.nucypher.kafka.zk.ClientType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
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
                    Send send = null;
                    switch (requestOrResponse.clientType) {
                        case PRODUCER:
                            send = recreateProduceRequest(requestOrResponse.destination,
                                    requestOrResponse.requestHeader,
                                    requestOrResponse.produceRequest,
                                    requestOrResponse.principalName,
                                    requestOrResponse.messageTransformer);
                            break;
                        case CONSUMER:
                            send = recreateFetchResponse(requestOrResponse.destination,
                                    requestOrResponse.requestHeader,
                                    requestOrResponse.fetchResponse,
                                    requestOrResponse.principalName,
                                    requestOrResponse.messageTransformer);
                            break;
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
                                        String principalName,
                                        MessageTransformer messageTransformer) {
        try {
            Map<TopicPartition, MemoryRecords> produceRecordsByPartition =
                    transformRecords(produceRequest, principalName, messageTransformer);
            ProduceRequest newProduceRequest = new ProduceRequest.Builder(
                    RecordBatch.CURRENT_MAGIC_VALUE,
                    produceRequest.acks(),
                    produceRequest.timeout(),
                    produceRecordsByPartition).build(header.apiVersion());
            return newProduceRequest.toSend(destination, header);
        } catch (Exception e) {
            LOGGER.warn("Error while updating produce request", e);
            Map<TopicPartition, MemoryRecords> produceRecordsByPartition =
                    produceRequest.partitionRecordsOrFail();
            Map<TopicPartition, ProduceResponse.PartitionResponse> errors =
                    new HashMap<>(produceRecordsByPartition.size());
            for (TopicPartition topicPartition : produceRecordsByPartition.keySet()) {
                errors.put(topicPartition,
                        new ProduceResponse.PartitionResponse(Errors.UNKNOWN));
            }
            ProduceResponse produceResponse = new ProduceResponse(errors);
            return produceResponse.toSend(Utils.getDestination(destination), header);
        }
    }

    private Map<TopicPartition, MemoryRecords> transformRecords(
            ProduceRequest produceRequest,
            String principalName,
            MessageTransformer messageTransformer) {
        Map<TopicPartition, MemoryRecords> produceRecordsByPartition =
                produceRequest.partitionRecordsOrFail();
        for (Map.Entry<TopicPartition, MemoryRecords> entry :
                produceRecordsByPartition.entrySet()) {
            MemoryRecords records = entry.getValue();
            String topic = entry.getKey().topic();
            if (records.sizeInBytes() == 0) {
                LOGGER.debug(
                        "Sends empty records to the topic '{}' without encryption",
                        entry.getKey().topic());
            } else if (!messageTransformer.shouldTransform(
                    topic, principalName, ClientType.PRODUCER)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Sends records to the topic '{}' without encryption",
                            entry.getKey().topic());
                }
            } else if (!messageTransformer.isAllowTransformation(
                    topic, principalName, ClientType.PRODUCER)) {
                //TODO split errors for different topics if possible
                throw new CommonException("Producer '%s' not authorized for writing " +
                        "records to the topic '%s'",
                        principalName, topic);
            } else {
                MemoryRecords newRecords = transformRecords(
                        topic,
                        records,
                        ENCRYPT_FACTOR,
                        principalName,
                        ClientType.PRODUCER,
                        messageTransformer);
                entry.setValue(newRecords);
            }
        }
        return produceRecordsByPartition;
    }

    private Send recreateFetchResponse(String destination,
                                       RequestHeader header,
                                       FetchResponse response,
                                       String principalName,
                                       MessageTransformer messageTransformer) {
        try {
            LinkedHashMap<TopicPartition, FetchResponse.PartitionData> data =
                    transformRecords(response, principalName, messageTransformer);
            response = new FetchResponse(data, response.throttleTimeMs());
            return response.toSend(destination, header);
        } catch (Exception e) {
            LOGGER.warn("Error while updating fetch response", e);
            LinkedHashMap<TopicPartition, FetchResponse.PartitionData> data =
                    response.responseData();
            LinkedHashMap<TopicPartition, FetchResponse.PartitionData> errors =
                    new LinkedHashMap<>(data.size());
            for (TopicPartition topicPartition : data.keySet()) {
                errors.put(topicPartition, new FetchResponse.PartitionData(
                        Errors.UNKNOWN,
                        FetchResponse.INVALID_HIGHWATERMARK,
                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                        FetchResponse.INVALID_LOG_START_OFFSET,
                        null,
                        MemoryRecords.EMPTY));
            }
            response = new FetchResponse(errors, response.throttleTimeMs());
            return response.toSend(destination, header);
        }
    }

    private LinkedHashMap<TopicPartition, FetchResponse.PartitionData> transformRecords(
            FetchResponse response,
            String principalName,
            MessageTransformer messageTransformer) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData> data =
                response.responseData();
        for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : data.entrySet()) {
            FetchResponse.PartitionData partitionData = entry.getValue();
            Records records = partitionData.records;
            String topic = entry.getKey().topic();
            if (records.sizeInBytes() == 0) {
                LOGGER.debug(
                        "Sends empty records from the topic '{}' without encryption",
                        entry.getKey().topic());
            } else if (!messageTransformer.shouldTransform(
                    topic, principalName, ClientType.CONSUMER)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Sends records from the topic '{}' without encryption",
                            entry.getKey().topic());
                }
            } else if (!messageTransformer.isAllowTransformation(
                    topic, principalName, ClientType.CONSUMER)) {
                //TODO split errors for different topics if possible
                throw new CommonException("Consumer '%s' not authorized for fetching " +
                        "records from the topic '%s'",
                        principalName, topic);
            } else {
                Records newRecords = transformRecords(
                        topic,
                        records,
                        DECRYPT_FACTOR,
                        principalName,
                        ClientType.CONSUMER,
                        messageTransformer);
                entry.setValue(new FetchResponse.PartitionData(
                        partitionData.error,
                        partitionData.highWatermark,
                        partitionData.lastStableOffset,
                        partitionData.logStartOffset,
                        partitionData.abortedTransactions,
                        newRecords));
            }
        }
        return data;
    }

    private MemoryRecords transformRecords(String topic,
                                           Records records,
                                           int sizeFactor,
                                           String principalName,
                                           ClientType clientType,
                                           MessageTransformer messageTransformer) {
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
                ByteBuffer value = record.value();
                byte[] valueBytes = null;
                if (value != null) {
                    valueBytes = new byte[value.remaining()];
                    value.get(valueBytes);
                }
                valueBytes = messageTransformer.transform(
                        topic, valueBytes, principalName, clientType);

                ByteBuffer key = record.key();
                byte[] keyBytes = null;
                if (key != null) {
                    keyBytes = new byte[key.remaining()];
                    key.get(keyBytes);
                }

                builder.appendWithOffset(
                        record.offset(),
                        record.timestamp(),
                        keyBytes,
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
        private final FetchResponse fetchResponse;
        private final String principalName;
        private final ClientType clientType;
        private final MessageTransformer messageTransformer;

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
                          ProduceRequest produceRequest,
                          Serializer<byte[]> serializer,
                          String principalName) {
            this.processor = processor;
            this.destination = destination;
            this.requestHeader = requestHeader;
            this.produceRequest = produceRequest;
            messageTransformer = new MessageTransformer.Serializer(serializer);
            clientType = ClientType.PRODUCER;
            fetchResponse = null;
            this.principalName = principalName;
        }

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
                          FetchResponse fetchResponse,
                          Deserializer<byte[]> deserializer,
                          String principalName) {
            this.processor = processor;
            this.destination = destination;
            this.fetchResponse = fetchResponse;
            this.requestHeader = requestHeader;
            messageTransformer = new MessageTransformer.Deserializer(deserializer);
            clientType = ClientType.CONSUMER;
            produceRequest = null;
            this.principalName = principalName;
        }

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
                          ProduceRequest produceRequest,
                          ReEncryptionHandler reEncryptionHandler,
                          String principalName) {
            this.processor = processor;
            this.destination = destination;
            this.requestHeader = requestHeader;
            this.produceRequest = produceRequest;
            messageTransformer = new MessageTransformer.ReEncryptor(reEncryptionHandler);
            clientType = ClientType.PRODUCER;
            fetchResponse = null;
            this.principalName = principalName;
        }

        RequestOrResponse(Processor processor,
                          String destination,
                          RequestHeader requestHeader,
                          FetchResponse fetchResponse,
                          ReEncryptionHandler reEncryptionHandler,
                          String principalName) {
            this.processor = processor;
            this.destination = destination;
            this.fetchResponse = fetchResponse;
            this.requestHeader = requestHeader;
            messageTransformer = new MessageTransformer.ReEncryptor(reEncryptionHandler);
            clientType = ClientType.CONSUMER;
            produceRequest = null;
            this.principalName = principalName;
        }
    }
}
