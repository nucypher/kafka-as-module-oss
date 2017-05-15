package com.nucypher.kafka.clients.encrypt.aes;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.encrypt.ByteEncryptorAndParametersFactory;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;

import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;

/**
 * {@link AesGcmCipherEncryptor} factory
 */
public class AesGcmCipherEncryptorFactory implements ByteEncryptorAndParametersFactory {

    private EncryptionAlgorithm algorithm;
    private PublicKey key;

    /**
     * Create factory without key
     */
    public AesGcmCipherEncryptorFactory() {

    }

    /**
     * @param algorithm encryption algorithm
     * @param key       public key
     */
    public AesGcmCipherEncryptorFactory(EncryptionAlgorithm algorithm, PublicKey key) {
        this.algorithm = algorithm;
        this.key = key;
    }

    @Override
    public Pair<ByteEncryptor, ExtraParameters> getInstance(
            EncryptionAlgorithm algorithm, PublicKey key) {
        try {
            AesGcmCipherEncryptor encryptor = new AesGcmCipherEncryptor(algorithm, key);
            return new Pair<ByteEncryptor, ExtraParameters>(encryptor, encryptor);
        } catch (InvalidAlgorithmParameterException | NoSuchAlgorithmException |
                NoSuchProviderException | NoSuchPaddingException | InvalidKeyException e) {
            throw new CommonException(e);
        }
    }

    @Override
    public Pair<ByteEncryptor, ExtraParameters> getInstance() {
        if (algorithm == null) {
            throw new CommonException("Algorithm must not be null");
        }
        if (key == null) {
            throw new CommonException("Key must not be null");
        }
        return getInstance(algorithm, key);
    }

}
