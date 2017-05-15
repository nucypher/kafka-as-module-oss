package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.encrypt.ByteEncryptor;

/**
 * Do not encrypt everything
 */
public class NonByteEncryptor implements ByteEncryptor {
    @Override
    public byte[] translate(byte[] data) {
        return data;
    }
}
