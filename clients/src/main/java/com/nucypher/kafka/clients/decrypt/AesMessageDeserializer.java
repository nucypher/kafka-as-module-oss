package com.nucypher.kafka.clients.decrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.cipher.CipherFactory;
import com.nucypher.kafka.cipher.ICipher;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.clients.MessageSerDeConfig;
import com.nucypher.kafka.clients.encrypt.AesMessageSerializerConfig;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithmUtils;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.Map;

/**
 * The message {@link Deserializer} which uses AES and encryption algorithm
 *
 * @param <T> type to be deserialized into.
 */
public class AesMessageDeserializer<T> implements Deserializer<T> {

    /**
     * Configured flag
     */
    private boolean isConfigured;
    /**
     * Message handler
     */
    protected MessageHandler messageHandler;
    /**
     * Data deserializer
     */
    protected Deserializer<T> deserializer;

    /**
     * Constructor used by Kafka consumer
     */
    public AesMessageDeserializer() {
        isConfigured = false;
    }

    /**
     * @param deserializer   Kafka deserializer
     * @param algorithmClass class of encryption algorithm
     * @param privateKey     EC private key
     */
    public AesMessageDeserializer(Deserializer<T> deserializer,
                                  Class<? extends EncryptionAlgorithm> algorithmClass,
                                  PrivateKey privateKey) {
        this(deserializer, algorithmClass, privateKey,
                null, null, null);
    }

    /**
     * Common constructor
     *
     * @param deserializer            Kafka deserializer
     * @param algorithmClass          class of encryption algorithm
     * @param privateKey              EC private key
     * @param decryptionCacheCapacity decryption cache capacity
     * @param provider                data encryption provider
     * @param transformation          data transformation
     */
    public AesMessageDeserializer(Deserializer<T> deserializer,
                                  Class<? extends EncryptionAlgorithm> algorithmClass,
                                  PrivateKey privateKey,
                                  Integer decryptionCacheCapacity,
                                  CipherFactory.CipherProvider provider,
                                  String transformation) {
        this.deserializer = deserializer;
        EncryptionAlgorithm algorithm =
                EncryptionAlgorithmUtils.getEncryptionAlgorithmByClass(algorithmClass);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm, privateKey, decryptionCacheCapacity);
        if (provider == null) {
            provider = CipherFactory.CipherProvider.valueOf(
                    MessageSerDeConfig.DATA_ENCRYPTION_PROVIDER_DEFAULT);
        }
        if (transformation == null) {
            transformation = MessageSerDeConfig.DATA_ENCRYPTION_TRANFORMATION_DEFAULT;
        }
        ICipher cipher = CipherFactory.getCipher(provider, transformation);
        messageHandler = new MessageHandler(cipher, keyManager, null);
        isConfigured = true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (!isConfigured) {
            AbstractConfig config = new AesMessageDeserializerConfig(configs);

            String path = config.getString(
                    AesMessageDeserializerConfig.PRIVATE_KEY_CONFIG);
            PrivateKey privateKey;
            try {
                privateKey = KeyUtils.getECKeyPairFromPEM(path).getPrivate();
            } catch (IOException e) {
                throw new CommonException(e);
            }

            Integer cacheCapacity = config.getInt(
                    AesMessageDeserializerConfig.CACHE_DECRYPTION_CAPACITY_CONFIG);
            EncryptionAlgorithm algorithm = EncryptionAlgorithmUtils.getEncryptionAlgorithm(
                    config.getString(AesMessageDeserializerConfig.DEK_ENCRYPTION_ALGORITHM_CONFIG));
            DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                    algorithm, privateKey, cacheCapacity);
            CipherFactory.CipherProvider provider = CipherFactory.CipherProvider.valueOf(
                    config.getString(AesMessageSerializerConfig.DATA_ENCRYPTION_PROVIDER_CONFIG)
                            .toUpperCase());
            String transformation = config.getString(
                    AesMessageSerializerConfig.DATA_ENCRYPTION_TRANFORMATION_CONFIG);
            ICipher cipher = CipherFactory.getCipher(provider, transformation);
            messageHandler = new MessageHandler(cipher, keyManager, null);

            if (isKey) {
                deserializer = config.getConfiguredInstance(
                        AesMessageDeserializerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
            } else {
                deserializer = config.getConfiguredInstance(
                        AesMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        Deserializer.class);
            }
        }
        deserializer.configure(configs, isKey);
        isConfigured = true;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        byte[] decrypted = messageHandler.decrypt(data);
        return deserializer.deserialize(topic, decrypted);
    }

    @Override
    public void close() {
        if (deserializer != null) {
            deserializer.close();
        }
    }
}
