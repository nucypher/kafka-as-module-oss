package com.nucypher.kafka.proxy.handler;

import com.nucypher.kafka.clients.ReEncryptionHandler;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.zk.ClientType;

/**
 * Interface for message transformer
 */
interface MessageTransformer {

    /**
     * Transform data
     *
     * @param topic         topic
     * @param data          data bytes
     * @param principalName Kafka principal name
     * @param clientType    client type
     * @return transformed data
     */
    public byte[] transform(String topic,
                            byte[] data,
                            String principalName,
                            ClientType clientType);

    /**
     * Checks the need for transformation
     *
     * @param topic         topic to check
     * @param principalName Kafka principal name
     * @param clientType    client type
     * @return result of checking
     */
    public boolean shouldTransform(String topic,
                                   String principalName,
                                   ClientType clientType);

    /**
     * Checks rights for records tranformation
     *
     * @param topic         topic name to check
     * @param principalName principal name to check
     * @param clientType    client type to check
     * @return result of checking
     */
    public boolean isAllowTransformation(String topic,
                                         String principalName,
                                         ClientType clientType);

    /**
     * Serializer
     */
    public static class Serializer implements MessageTransformer {

        private org.apache.kafka.common.serialization.Serializer<byte[]> serializer;

        public Serializer(org.apache.kafka.common.serialization.Serializer<byte[]> serializer) {
            this.serializer = serializer;
        }

        @Override
        public byte[] transform(String topic,
                                byte[] data,
                                String principalName,
                                ClientType clientType) {
            if (clientType != ClientType.PRODUCER) {
                throw new CommonException(
                        "Serialization is only available for client type " +
                                ClientType.PRODUCER);
            }
            return serializer.serialize(topic, data);
        }

        @Override
        public boolean shouldTransform(String topic,
                                       String principalName,
                                       ClientType clientType) {
            return true;
        }

        @Override
        public boolean isAllowTransformation(String topic,
                                             String principalName,
                                             ClientType clientType) {
            return true;
        }
    }

    /**
     * Deserializer
     */
    public static class Deserializer implements MessageTransformer {

        private org.apache.kafka.common.serialization.Deserializer<byte[]> deserializer;

        public Deserializer(org.apache.kafka.common.serialization.Deserializer<byte[]> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public byte[] transform(String topic,
                                byte[] data,
                                String principalName,
                                ClientType clientType) {
            if (clientType != ClientType.CONSUMER) {
                throw new CommonException(
                        "Deserialization is only available for client type " +
                                ClientType.CONSUMER);
            }
            return deserializer.deserialize(topic, data);
        }

        @Override
        public boolean shouldTransform(String topic,
                                       String principalName,
                                       ClientType clientType) {
            return true;
        }

        @Override
        public boolean isAllowTransformation(String topic,
                                             String principalName,
                                             ClientType clientType) {
            return true;
        }
    }

    /**
     * Re-encryptor
     */
    public static class ReEncryptor implements MessageTransformer {

        private ReEncryptionHandler handler;

        public ReEncryptor(ReEncryptionHandler handler) {
            this.handler = handler;
        }

        @Override
        public byte[] transform(String topic,
                                byte[] data,
                                String principalName,
                                ClientType clientType) {
            return handler.reEncryptTopic(data, topic, principalName, clientType);
        }

        @Override
        public boolean shouldTransform(String topic,
                                       String principalName,
                                       ClientType clientType) {
            return handler.isTopicEncrypted(topic, principalName, clientType);
        }

        @Override
        public boolean isAllowTransformation(String topic,
                                             String principalName,
                                             ClientType clientType) {
            return handler.isAllowReEncryption(topic, principalName, clientType);
        }
    }
}
