package com.nucypher.kafka.decrypt;

import java.security.PublicKey;

/**
 * {@link ByteDecryptor} factory
 */
public interface ByteDecryptorFactory {

    /**
     * Get instance of {@link ByteDecryptor}
     *
     * @param key private key
     * @return {@link ByteDecryptor}
     */
    public ByteDecryptor getInstance(PublicKey key);

    /**
     * Get instance of {@link ByteDecryptor}
     *
     * @return {@link ByteDecryptor}
     */
    public ByteDecryptor getInstance();

}
