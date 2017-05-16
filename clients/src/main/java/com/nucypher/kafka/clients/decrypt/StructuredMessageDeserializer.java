package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessage;
import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.errors.CommonException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * The structured message {@link Deserializer}
 *
 * @param <T> type to be deserialized into.
 */
public class StructuredMessageDeserializer<T> implements Deserializer<T> {

    private Deserializer<T> deserializer;
    private StructuredDataAccessor accessor;
    protected ByteDecryptor decryptor;

    /**
     * Constructor used by Kafka consumer
     */
    public StructuredMessageDeserializer() {

    }

    //TODO add configs to other constructors

    /**
     * @param deserializer      Kafka deserializer
     * @param decryptor         decryptor
     * @param dataAccessorClass data accessor class
     */
    public StructuredMessageDeserializer(
            Deserializer<T> deserializer,
            ByteDecryptor decryptor,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this.deserializer = deserializer;
        this.decryptor = decryptor;
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
    }

    /**
     * @param deserializer Kafka deserializer
     * @param decryptor    decryptor
     * @param format       data format
     */
    public StructuredMessageDeserializer(
            Deserializer<T> deserializer,
            ByteDecryptor decryptor,
            DataFormat format) {
        this(deserializer, decryptor, format.getAccessorClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (decryptor == null) {
            throw new CommonException("Decryptor is not initialized");
        }
        if (deserializer == null || accessor == null) {
            StructuredMessageDeserializerConfig config =
                    new StructuredMessageDeserializerConfig(configs);
            if (isKey) {
                deserializer = config.getConfiguredInstance(
                        StructuredMessageDeserializerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
            } else {
                deserializer = config.getConfiguredInstance(
                        StructuredMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
            }
            accessor = config.getConfiguredInstance(
                    StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
        }
        deserializer.configure(configs, isKey);
        accessor.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            StructuredMessage message = new StructuredMessage(
                    data, topic, accessor, decryptor);
            byte[] decrypted = message.decrypt();
            return deserializer.deserialize(topic, decrypted);
        } catch (Exception e) {
            throw new CommonException("Error while decrypting", e);
        }
    }

    @Override
    public void close() {
        if (deserializer != null) {
            deserializer.close();
        }
    }
}
