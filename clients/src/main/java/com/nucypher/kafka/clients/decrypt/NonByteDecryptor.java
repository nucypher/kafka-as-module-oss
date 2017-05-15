package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.decrypt.ByteDecryptor;

/**
 * Do not decrypt everything
 */
public class NonByteDecryptor implements ByteDecryptor {

    @Override
    public byte[] translate(byte[] data) {
        return data;
    }
}
