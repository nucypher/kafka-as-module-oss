package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.errors.client.EncryptorException;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.utils.ByteUtils;

/**
 */
public class InverseByteByteEncryptor implements ByteEncryptor {
    @Override
    public byte[] translate(byte[] data) {
        if (data == null) {
            throw new EncryptorException("Unable to encrypt empty data");
        }
        return ByteUtils.invert(data);
    }
}
