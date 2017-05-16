package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessage;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.encrypt.ByteEncryptorAndParametersFactory;
import com.nucypher.kafka.errors.CommonException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for the structured message serialization
 *
 * @param <T> Type to be serialized from.
 */
public class StructuredMessageSerializer<T> implements Serializer<T> {

    private Serializer<T> serializer;
    private Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors;
    private StructuredDataAccessor accessor;

    protected ByteEncryptorAndParametersFactory encryptorFactory;

    /**
     * Constructor used by Kafka producer
     */
    public StructuredMessageSerializer() {

    }

    //TODO add configs to other constructors

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer        Kafka serializer
     * @param encryptorFactory  encryptor factory
     * @param dataAccessorClass data accessor class
     */
    public StructuredMessageSerializer(
            Serializer<T> serializer,
            ByteEncryptorAndParametersFactory encryptorFactory,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this.serializer = serializer;
        this.encryptorFactory = encryptorFactory;
        accessor = getAccessorInstance(dataAccessorClass);
    }

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer       Kafka serializer
     * @param encryptorFactory encryptor factory
     * @param format           data format
     */
    public StructuredMessageSerializer(
            Serializer<T> serializer,
            ByteEncryptorAndParametersFactory encryptorFactory,
            DataFormat format) {
        this(serializer, encryptorFactory, format.getAccessorClass());
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer        Kafka serializer
     * @param fields            collection of fields to encryption
     * @param factory           encryptor factory
     * @param dataAccessorClass data accessor class
     */
    public StructuredMessageSerializer(
            Serializer<T> serializer,
            Collection<String> fields,
            ByteEncryptorAndParametersFactory factory,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this(serializer, factory, dataAccessorClass);
        initializeEncryptors(fields);
    }

    private void initializeEncryptors(Collection<String> fields) {
        if (fields != null && fields.size() > 0) {
            encryptors = new HashMap<>(fields.size());
            for (String field : fields) {
                encryptors.put(field, encryptorFactory.getInstance());
            }
        }
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer Kafka serializer
     * @param fields     collection of fields to encryption
     * @param factory    encryptor factory
     * @param format     data format
     */
    public StructuredMessageSerializer(
            Serializer<T> serializer,
            Collection<String> fields,
            ByteEncryptorAndParametersFactory factory,
            DataFormat format) {
        this(serializer, fields, factory, format.getAccessorClass());
    }

    /**
     * Constructor for encrypting specified fields with specified encryptors
     *
     * @param serializer        Kafka serializer
     * @param encryptors        fields and their encryptors
     * @param dataAccessorClass data accessor class
     */
    public StructuredMessageSerializer(
            Serializer<T> serializer,
            Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this.serializer = serializer;
        this.encryptors = encryptors;
        accessor = getAccessorInstance(dataAccessorClass);
    }

    private static StructuredDataAccessor getAccessorInstance(
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        try {
            return dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (encryptorFactory == null && encryptors == null) {
            throw new CommonException("Encryption factory and encryptors are not initialized");
        }
        if (serializer == null || accessor == null) {
            StructuredMessageSerializerConfig config = new StructuredMessageSerializerConfig(configs);
            if (isKey) {
                serializer = config.getConfiguredInstance(
                        StructuredMessageSerializerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
            } else {
                serializer = config.getConfiguredInstance(
                        StructuredMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
            }
            accessor = config.getConfiguredInstance(
                    StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
            initializeEncryptors(
                    config.getList(StructuredMessageSerializerConfig.FIELDS_LIST_CONFIG));
        }
        serializer.configure(configs, isKey);
        accessor.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            byte[] serialized = serializer.serialize(topic, data);
            if (encryptors == null) {
                encryptors = new HashMap<>();
                accessor.deserialize(topic, serialized);
                for (String field : accessor.getAllFields()) {
                    encryptors.put(field, encryptorFactory.getInstance());
                }
            }

            StructuredMessage message = new StructuredMessage(
                    serialized, topic, accessor, encryptors);
            return message.encrypt();
        } catch (IOException | ClassNotFoundException |
                IllegalAccessException | InstantiationException e) {
            throw new CommonException("Error while encrypting", e);
        }
    }

    @Override
    public void close() {
        if (serializer != null) {
            serializer.close();
        }
    }
}
