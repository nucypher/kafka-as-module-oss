package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.encrypt.ByteEncryptor;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Predefined String ByteEncryptor with org.apache.kafka.common.serialization.StringSerializer
 */
public class StringByteEncryptorSerializer extends ByteEncryptorSerializer<String> {

    public StringByteEncryptorSerializer(ByteEncryptor byteEncryptor) {
        super(new StringSerializer(), byteEncryptor);
    }

    public StringByteEncryptorSerializer(Serializer<String> serializer) {
        super(serializer);
    }

    public StringByteEncryptorSerializer(Serializer<String> serializer, ByteEncryptor byteEncryptor) {
        super(serializer, byteEncryptor);
    }
}
