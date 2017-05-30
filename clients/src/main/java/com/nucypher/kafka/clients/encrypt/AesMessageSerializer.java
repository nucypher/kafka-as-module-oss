package com.nucypher.kafka.clients.encrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.cipher.AesGcmCipher;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithmUtils;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Map;

/**
 * The message {@link Serializer} which uses AES and encryption algorithm
 *
 * @param <T> type to be serialized from.
 */
public class AesMessageSerializer<T> implements Serializer<T> {

    /**
     * Configured flag
     */
    protected boolean isConfigured;
    /**
     * Message handler
     */
    protected MessageHandler messageHandler;
    /**
     * Data serializer
     */
    protected Serializer<T> serializer;

    /**
     * Constructor used by Kafka producer
     */
    public AesMessageSerializer() {
        isConfigured = false;
    }

    /**
     * @param serializer     Kafka serializer
     * @param algorithmClass class of encryption algorithm
     * @param publicKey      public key
     * @param maxUsingDEK    max number of using each DEK
     */
    public AesMessageSerializer(Serializer<T> serializer,
                                Class<? extends EncryptionAlgorithm> algorithmClass,
                                PublicKey publicKey,
                                Integer maxUsingDEK) {
        this.serializer = serializer;
        SecureRandom secureRandom = new SecureRandom();
        EncryptionAlgorithm algorithm =
                EncryptionAlgorithmUtils.getEncryptionAlgorithmByClass(algorithmClass);
        DataEncryptionKeyManager keyManager =
                new DataEncryptionKeyManager(algorithm, publicKey, secureRandom, maxUsingDEK);
        AesGcmCipher cipher = new AesGcmCipher();
        messageHandler = new MessageHandler(cipher, keyManager, secureRandom);
        isConfigured = true;
    }


    /**
     * Common constructor
     *
     * @param serializer              Kafka serializer
     * @param algorithmClass          class of encryption algorithm
     * @param publicKey               public key
     * @param maxUsingDEK             max number of using each DEK
     * @param encryptionCacheCapacity encryption cache capacity
     */
    public AesMessageSerializer(Serializer<T> serializer,
                                Class<? extends EncryptionAlgorithm> algorithmClass,
                                PublicKey publicKey,
                                Integer maxUsingDEK,
                                Integer encryptionCacheCapacity) {
        this.serializer = serializer;
        SecureRandom secureRandom = new SecureRandom();
        EncryptionAlgorithm algorithm =
                EncryptionAlgorithmUtils.getEncryptionAlgorithmByClass(algorithmClass);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm, publicKey, secureRandom, maxUsingDEK, encryptionCacheCapacity);
        AesGcmCipher cipher = new AesGcmCipher();
        messageHandler = new MessageHandler(cipher, keyManager, secureRandom);
        isConfigured = true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (!isConfigured) {
            AbstractConfig config = new AesMessageSerializerConfig(configs);

            String path = config.getString(
                    AesMessageSerializerConfig.PUBLIC_KEY_CONFIG);
            KeyPair keyPair;
            try {
                keyPair = KeyUtils.getECKeyPairFromPEM(path);
            } catch (IOException e) {
                throw new CommonException(e);
            }

            Integer maxUsingDEK = config.getInt(
                    AesMessageSerializerConfig.MAX_USING_DEK_CONFIG);
            Integer cacheCapacity = config.getInt(
                    AesMessageSerializerConfig.CACHE_ENCRYPTION_CAPACITY_CONFIG);
            EncryptionAlgorithm algorithm = EncryptionAlgorithmUtils.getEncryptionAlgorithm(
                    config.getString(AesMessageSerializerConfig.ALGORITHM_CONFIG));
            SecureRandom secureRandom = new SecureRandom();
            DataEncryptionKeyManager keyManager = getKeyManager(
                    config, keyPair, maxUsingDEK, cacheCapacity, algorithm, secureRandom);
            AesGcmCipher cipher = new AesGcmCipher();
            messageHandler = new MessageHandler(cipher, keyManager, secureRandom);

            if (isKey) {
                serializer = config.getConfiguredInstance(
                        AesMessageSerializerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
            } else {
                serializer = config.getConfiguredInstance(
                        AesMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
            }
        }
        serializer.configure(configs, isKey);
        isConfigured = true;
    }

    /**
     * Get key manager
     *
     * @param config        configuration
     * @param keyPair       key pair
     * @param maxUsingDEK   max number of using each DEK
     * @param cacheCapacity cache capacity
     * @param algorithm     algorithm
     * @param secureRandom  secure random
     * @return key manager
     */
    protected DataEncryptionKeyManager getKeyManager(AbstractConfig config,
                                                     KeyPair keyPair,
                                                     Integer maxUsingDEK,
                                                     Integer cacheCapacity,
                                                     EncryptionAlgorithm algorithm,
                                                     SecureRandom secureRandom) {
        return new DataEncryptionKeyManager(
                algorithm, keyPair.getPublic(), secureRandom, maxUsingDEK, cacheCapacity);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] bytes = serializer.serialize(topic, data);
        return messageHandler.encrypt(topic, bytes);
    }

    @Override
    public void close() {
        if (serializer != null) {
            serializer.close();
        }
    }
}
