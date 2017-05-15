package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.errors.client.DecryptorException;
import com.nucypher.kafka.utils.ByteUtils;

/**
 */
public class InverseByteDecryptor implements ByteDecryptor {

    @Override
    public byte[] translate(byte[] data) {
        if (data == null) {
            throw new DecryptorException("Unable to decrypt empty data");
        }
        return ByteUtils.invert(data);
    }
}
