package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.Map;

/**
 * The structured message {@link Deserializer} which uses AES and encryption algorithm
 *
 * @param <T> ype to be deserialized into.
 */
public class AesStructuredMessageDeserializer<T> extends StructuredMessageDeserializer<T> {

    /**
     * Constructor used by Kafka consumer
     */
    public AesStructuredMessageDeserializer() {

    }

    //TODO add configs to other constructors

    /**
     * @param deserializer      Kafka deserializer
     * @param privateKey        EC private key
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            PrivateKey privateKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        super(deserializer, new AesGcmCipherDecryptor(privateKey), dataAccessorClass);
    }

    /**
     * @param deserializer Kafka deserializer
     * @param privateKey   EC private key
     * @param format       data format
     */
    public AesStructuredMessageDeserializer(
            Deserializer<T> deserializer,
            PrivateKey privateKey,
            DataFormat format) {
        super(deserializer, new AesGcmCipherDecryptor(privateKey), format);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (decryptor == null) {
            DefaultProvider.initializeProvider();
            AbstractConfig config = new AesStructuredMessageDeserializerConfig(configs);
            String path = config.getString(
                    AesStructuredMessageDeserializerConfig.PRIVATE_KEY_CONFIG);
            PrivateKey privateKey;
            try {
                privateKey = KeyUtils.getECKeyPairFromPEM(path).getPrivate();
            } catch (IOException e) {
                throw new CommonException(e);
            }
            decryptor = new AesGcmCipherDecryptor(privateKey);
        }
        super.configure(configs, isKey);
    }
}
