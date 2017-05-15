package com.nucypher.kafka.clients.decrypt.aes;

import com.nucypher.kafka.cipher.CryptType;
import com.nucypher.kafka.cipher.ICipher;
import com.nucypher.kafka.clients.AbstractAesGcmCipher;
import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.decrypt.StreamDecryptor;

import java.security.PrivateKey;
import java.security.PublicKey;

/**
 *
 */
public class AesGcmCipherDecryptor extends AbstractAesGcmCipher implements ByteDecryptor, StreamDecryptor, ICipher {

    private PrivateKey privateKey;

    /**
     * Cipher initialization is lazy (vs to AesGcmCipherEncryptor) 'cause need to get DEK and IV
     * @param privateKey
     */
    public AesGcmCipherDecryptor(PrivateKey privateKey) {
        super(CryptType.DECRYPT);
        this.privateKey = privateKey;
    }

    @Override
    public PrivateKey getPrivateKey() {
        return this.privateKey;
    }

    @Override
    public PublicKey getPublicKey() {
        throw new UnsupportedOperationException("Public key is not support by Decryptor");
    }
}
