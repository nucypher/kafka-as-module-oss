package com.nucypher.kafka.encrypt;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.utils.EncryptionAlgorithm;

import java.security.PublicKey;

/**
 * {@link ByteEncryptor} and {@link ExtraParameters} factory
 */
public interface ByteEncryptorAndParametersFactory {

    /**
     * Get instance of {@link ByteEncryptor} and {@link ExtraParameters}
     *
     * @param algorithm encryption algorithm
     * @param key       public key
     * @return {@link ByteEncryptor} and {@link ExtraParameters}
     */
    public Pair<ByteEncryptor, ExtraParameters> getInstance(
            EncryptionAlgorithm algorithm, PublicKey key);

    /**
     * Get instance of {@link ByteEncryptor} and {@link ExtraParameters}
     *
     * @return {@link ByteEncryptor} and {@link ExtraParameters}
     */
    public Pair<ByteEncryptor, ExtraParameters> getInstance();

}
