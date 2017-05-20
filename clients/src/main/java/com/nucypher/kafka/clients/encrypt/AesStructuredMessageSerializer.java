package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessageHandler;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.security.PublicKey;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The structured message {@link Serializer} which uses AES and encryption algorithm
 *
 * @param <T> Type to be serialized from.
 */
public class AesStructuredMessageSerializer<T> extends AesMessageSerializer<T> {

    private StructuredDataAccessor accessor;
    private StructuredMessageHandler structuredMessageHandler;
    private Set<String> fields;

    /**
     * Constructor used by Kafka producer
     */
    public AesStructuredMessageSerializer() {
        super();
    }

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer        Kafka serializer
     * @param algorithm         encryption algorithm
     * @param publicKey         EC public key
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this(serializer, algorithm, publicKey, dataAccessorClass, null);
    }

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer Kafka serializer
     * @param algorithm  encryption algorithm
     * @param publicKey  EC public key
     * @param format     data format
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            DataFormat format) {
        this(serializer, algorithm, publicKey, format.getAccessorClass(), null);
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer        Kafka serializer
     * @param algorithm         encryption algorithm
     * @param publicKey         EC public key
     * @param dataAccessorClass data accessor class
     * @param fields            collection of fields to encryption
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass,
            Set<String> fields) {
        super(serializer, algorithm, publicKey);
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
        structuredMessageHandler = new StructuredMessageHandler(messageHandler);
        this.fields = fields;
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer Kafka serializer
     * @param algorithm  encryption algorithm
     * @param publicKey  EC public key
     * @param format     data format
     * @param fields     collection of fields to encryption
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            DataFormat format,
            Set<String> fields) {
        this(serializer, algorithm, publicKey, format.getAccessorClass(), fields);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (!isConfigured) {
            AbstractConfig config = new AesStructuredMessageSerializerConfig(configs);
            accessor = config.getConfiguredInstance(
                    AesStructuredMessageSerializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
            structuredMessageHandler = new StructuredMessageHandler(messageHandler);
            List<String> fieldsList = config.getList(
                    AesStructuredMessageSerializerConfig.FIELDS_LIST_CONFIG);
            if (fieldsList != null) {
                fields = new HashSet<>(fieldsList);
            }
        }
        accessor.configure(configs, isKey);
        super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] serialized = serializer.serialize(topic, data);
        if (fields == null) {
            accessor.deserialize(topic, serialized);
            fields = accessor.getAllFields();
        }

        return structuredMessageHandler.encrypt(topic, serialized, accessor, fields);
    }
}
