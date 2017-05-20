package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.cipher.AesGcmCipher;
import com.nucypher.kafka.clients.MessageHandler;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
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
    protected boolean isConfigured;
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
     * @param deserializer Kafka deserializer
     * @param algorithm    encryption algorithm
     * @param privateKey   EC private key
     */
    public AesMessageDeserializer(Deserializer<T> deserializer,
                                  EncryptionAlgorithm algorithm,
                                  PrivateKey privateKey) {
        this.deserializer = deserializer;
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(algorithm, privateKey);
        AesGcmCipher cipher = new AesGcmCipher();
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
            EncryptionAlgorithm algorithm = EncryptionAlgorithm.valueOf(
                    config.getString(AesMessageDeserializerConfig.ALGORITHM_CONFIG));
            DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(algorithm, privateKey);
            AesGcmCipher cipher = new AesGcmCipher();
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
