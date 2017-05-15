package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptorFactory;
import com.nucypher.kafka.clients.granular.DataFormat;
import com.nucypher.kafka.clients.granular.StructuredDataAccessor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.security.PublicKey;
import java.util.Collection;
import java.util.Map;

/**
 * The structured message {@link Serializer} which uses AES and encryption algorithm
 *
 * @param <T> Type to be serialized from.
 */
public class AesStructuredMessageSerializer<T> extends StructuredMessageSerializer<T> {

    /**
     * Constructor used by Kafka producer
     */
    public AesStructuredMessageSerializer() {

    }

    //TODO add configs to other constructors

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
        super(serializer, new AesGcmCipherEncryptorFactory(algorithm, publicKey), dataAccessorClass);
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
        super(serializer, new AesGcmCipherEncryptorFactory(algorithm, publicKey), format);
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer        Kafka serializer
     * @param fields            collection of fields to encryption
     * @param algorithm         encryption algorithm
     * @param publicKey         EC public key
     * @param dataAccessorClass data accessor class
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Collection<String> fields,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            Class<? extends StructuredDataAccessor> dataAccessorClass) {
        super(serializer, fields,
                new AesGcmCipherEncryptorFactory(algorithm, publicKey), dataAccessorClass);
    }

    /**
     * Constructor for encrypting specified fields
     *
     * @param serializer Kafka serializer
     * @param fields     collection of fields to encryption
     * @param algorithm  encryption algorithm
     * @param publicKey  EC public key
     * @param format     data format
     */
    public AesStructuredMessageSerializer(
            Serializer<T> serializer,
            Collection<String> fields,
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            DataFormat format) {
        super(serializer, fields, new AesGcmCipherEncryptorFactory(algorithm, publicKey), format);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (encryptorFactory == null) {
            DefaultProvider.initializeProvider();
            AbstractConfig config = new AesStructuredMessageSerializerConfig(configs);
            String path = config.getString(
                    AesStructuredMessageSerializerConfig.PUBLIC_KEY_CONFIG);
            EncryptionAlgorithm algorithm = EncryptionAlgorithm.valueOf(config.getString(
                    AesStructuredMessageSerializerConfig.ALGORITHM_CONFIG));
            PublicKey publicKey;
            try {
                publicKey = KeyUtils.getECKeyPairFromPEM(path).getPublic();
            } catch (IOException e) {
                throw new CommonException(e);
            }
            encryptorFactory = new AesGcmCipherEncryptorFactory(algorithm, publicKey);
        }
        super.configure(configs, isKey);
    }
}
