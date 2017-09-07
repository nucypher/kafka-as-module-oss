package com.nucypher.kafka.clients.decrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessageHandler;
import com.nucypher.kafka.errors.CommonException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.security.PrivateKey;
import java.util.Map;

/**
 * The structured message {@link Deserializer} which uses AES and encryption algorithm
 *
 * @param <T> type to be deserialized into.
 */
public class AesStructuredMessageDeserializer<T> extends AesMessageDeserializer<T> {

    private StructuredDataAccessor accessor;
    private StructuredMessageHandler structuredMessageHandler;
    private boolean isConfigured;

    /**
     * Constructor used by Kafka consumer
     */
    public AesStructuredMessageDeserializer() {
        super();
        isConfigured = false;
    }

    /**
     * @param deserializer      Kafka deserializer
     * @param algorithmClass    class of encryption algorithm
     * @param privateKey        EC private key
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PrivateKey privateKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        super(deserializer, algorithmClass, privateKey);
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
        structuredMessageHandler = new StructuredMessageHandler(messageHandler);
        isConfigured = true;
    }

    /**
     * @param deserializer   Kafka deserializer
     * @param algorithmClass class of encryption algorithm
     * @param privateKey     EC private key
     * @param format         data format
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PrivateKey privateKey,
            DataFormat format) {
        this(deserializer, algorithmClass, privateKey, format.getAccessorClass());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        if (!isConfigured) {
            AbstractConfig config = new AesStructuredMessageDeserializerConfig(configs);
            accessor = config.getConfiguredInstance(
                    AesStructuredMessageDeserializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
            structuredMessageHandler = new StructuredMessageHandler(messageHandler);
            isConfigured = true;
        }
        accessor.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        byte[] decrypted = structuredMessageHandler.decrypt(topic, data, accessor);
        return deserializer.deserialize(topic, decrypted);
    }
}
