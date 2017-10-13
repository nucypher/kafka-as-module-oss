package com.nucypher.kafka.clients.encrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.cipher.CipherFactory;
import com.nucypher.kafka.cipher.ICipher;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.clients.MessageSerDeConfig;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.clients.granular.StructuredMessageHandler;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithmUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
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

    private boolean isConfigured;
    private StructuredDataAccessor accessor;
    private StructuredMessageHandler structuredMessageHandler;
    private Set<String> fields;

    /**
     * Constructor used by Kafka producer
     */
    public AesStructuredMessageSerializer() {
        super();
        isConfigured = false;
    }

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer        Kafka serializer
     * @param algorithmClass    class of encryption algorithm
     * @param publicKey         EC public key
     * @param maxUsingDEK       max number of using each DEK
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PublicKey publicKey,
            Integer maxUsingDEK,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        this(serializer, algorithmClass, publicKey, maxUsingDEK, dataAccessorClass, null);
    }

    /**
     * Constructor for encrypting all fields
     *
     * @param serializer     Kafka serializer
     * @param algorithmClass class of encryption algorithm
     * @param publicKey      EC public key
     * @param maxUsingDEK    max number of using each DEK
     * @param format         data format
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PublicKey publicKey,
            Integer maxUsingDEK,
            DataFormat format) {
        this(serializer, algorithmClass, publicKey, maxUsingDEK, format.getAccessorClass(), null);
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer        Kafka serializer
     * @param algorithmClass    class of encryption algorithm
     * @param publicKey         EC public key
     * @param maxUsingDEK       max number of using each DEK
     * @param dataAccessorClass data accessor class
     * @param fields            collection of fields to encryption
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PublicKey publicKey,
            Integer maxUsingDEK,
            Class<? extends StructuredDataAccessor> dataAccessorClass,
            Set<String> fields) {
        super(serializer, algorithmClass, publicKey, maxUsingDEK);
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
        structuredMessageHandler = new StructuredMessageHandler(messageHandler);
        this.fields = fields;
        isConfigured = true;
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer     Kafka serializer
     * @param algorithmClass class of encryption algorithm
     * @param publicKey      EC public key
     * @param maxUsingDEK    max number of using each DEK
     * @param format         data format
     * @param fields         collection of fields to encryption
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            PublicKey publicKey,
            Integer maxUsingDEK,
            DataFormat format,
            Set<String> fields) {
        this(serializer, algorithmClass, publicKey, maxUsingDEK, format.getAccessorClass(), fields);
    }

    /**
     * Common constructor
     *
     * @param serializer              Kafka serializer
     * @param algorithmClass          class of encryption algorithm
     * @param keyPair                 EC key pair
     * @param maxUsingDEK             max number of using each DEK
     * @param dataAccessorClass       data accessor class
     * @param fields                  collection of fields to encryption
     * @param useDerivedKeys          use derived keys
     * @param encryptionCacheCapacity encryption cache capacity
     * @param provider                data encryption provider
     * @param transformation          data transformation
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Class<? extends EncryptionAlgorithm> algorithmClass,
            KeyPair keyPair,
            Integer maxUsingDEK,
            Class<? extends StructuredDataAccessor> dataAccessorClass,
            Set<String> fields,
            boolean useDerivedKeys,
            Integer encryptionCacheCapacity,
            CipherFactory.CipherProvider provider,
            String transformation) {
        this.serializer = serializer;
        SecureRandom secureRandom = new SecureRandom();
        EncryptionAlgorithm algorithm =
                EncryptionAlgorithmUtils.getEncryptionAlgorithmByClass(algorithmClass);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm, keyPair, secureRandom, useDerivedKeys, maxUsingDEK, encryptionCacheCapacity);
        if (provider == null) {
            provider = CipherFactory.CipherProvider.valueOf(
                    MessageSerDeConfig.DATA_ENCRYPTION_PROVIDER_DEFAULT);
        }
        if (transformation == null) {
            transformation = MessageSerDeConfig.DATA_ENCRYPTION_TRANFORMATION_DEFAULT;
        }
        ICipher cipher = CipherFactory.getCipher(provider, transformation);
        messageHandler = new MessageHandler(cipher, keyManager, secureRandom);
        try {
            accessor = dataAccessorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new CommonException(e);
        }
        structuredMessageHandler = new StructuredMessageHandler(messageHandler);
        this.fields = fields;
        isConfigured = true;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        if (!isConfigured) {
            AbstractConfig config = getConfig(configs);
            accessor = config.getConfiguredInstance(
                    AesStructuredMessageSerializerConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                    StructuredDataAccessor.class);
            structuredMessageHandler = new StructuredMessageHandler(messageHandler);
            List<String> fieldsList = config.getList(
                    AesStructuredMessageSerializerConfig.FIELDS_LIST_CONFIG);
            if (fieldsList != null && !fieldsList.isEmpty()) {
                fields = new HashSet<>(fieldsList);
            }
            isConfigured = true;
        }
        accessor.configure(configs, isKey);
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

    @Override
    protected AbstractConfig getConfig(Map<String, ?> configs) {
        return new AesStructuredMessageSerializerConfig(configs);
    }

    @Override
    protected DataEncryptionKeyManager getKeyManager(AbstractConfig config,
                                                     KeyPair keyPair,
                                                     Integer maxUsingDEK,
                                                     Integer cacheCapacity,
                                                     EncryptionAlgorithm algorithm,
                                                     SecureRandom secureRandom) {
        boolean useDerivedKeys = config.getBoolean(
                AesStructuredMessageSerializerConfig.USE_DERIVED_KEYS_CONFIG);
        return new DataEncryptionKeyManager(
                algorithm, keyPair, secureRandom, useDerivedKeys, maxUsingDEK, cacheCapacity);
    }
}
