package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.decrypt.ByteDecryptor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 */
public class StringByteDecryptorDeserializer extends ByteDecryptorDeserializer<String> {

    public StringByteDecryptorDeserializer(ByteDecryptor byteDecryptor) {
        super(new StringDeserializer(), byteDecryptor);
    }

    public StringByteDecryptorDeserializer(Deserializer<String> deserializer) {
        super(deserializer);
    }

    public StringByteDecryptorDeserializer(Deserializer<String> deserializer, ByteDecryptor byteDecryptor) {
        super(deserializer, byteDecryptor);
    }
}
