package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessageHandler;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
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

    /**
     * Constructor used by Kafka consumer
     */
    public AesStructuredMessageDeserializer() {
        super();
    }

    /**
     * @param deserializer      Kafka deserializer
     * @param algorithm         encryption algorithm
     * @param privateKey        EC private key
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            EncryptionAlgorithm algorithm,
            PrivateKey privateKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        super(deserializer, algorithm, privateKey);
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
        structuredMessageHandler = new StructuredMessageHandler(messageHandler);
    }

    /**
     * @param deserializer Kafka deserializer
     * @param algorithm    encryption algorithm
     * @param privateKey   EC private key
     * @param format       data format
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            EncryptionAlgorithm algorithm,
            PrivateKey privateKey,
            DataFormat format) {
        this(deserializer, algorithm, privateKey, format.getAccessorClass());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (!isConfigured) {
            AbstractConfig config = new AesStructuredMessageDeserializerConfig(configs);
            accessor = config.getConfiguredInstance(
                    AesStructuredMessageDeserializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
            structuredMessageHandler = new StructuredMessageHandler(messageHandler);
        }
        accessor.configure(configs, isKey);
        super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        byte[] decrypted = structuredMessageHandler.decrypt(topic, data, accessor);
        return deserializer.deserialize(topic, decrypted);
    }
}
