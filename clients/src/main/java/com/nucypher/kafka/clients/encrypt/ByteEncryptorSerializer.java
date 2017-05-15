package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.errors.client.EmptyEncryptorException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Decorator for Kafka's Serializers plus ByteEncryptor as Strategy pattern
 */
@Slf4j
public class ByteEncryptorSerializer<T> implements ByteEncryptor, Serializer<T> {

    protected Serializer<T> serializer;
    private ByteEncryptor byteEncryptor;

    /**
     * @param serializer -
     */
    public ByteEncryptorSerializer(Serializer<T> serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer is empty");
        }
        this.serializer = serializer;
        this.byteEncryptor = new NonByteEncryptor();
    }

    /**
     * @param serializer -
     * @param byteEncryptor -
     */
    public ByteEncryptorSerializer(Serializer<T> serializer, ByteEncryptor byteEncryptor) {
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer is empty");
        }

        if (byteEncryptor == null) {
            throw new IllegalArgumentException("ByteEncryptor is empty");
        }
        this.serializer = serializer;
        this.byteEncryptor = byteEncryptor;
    }

    public Serializer<T> getSerializer() {
        return serializer;
    }

    public ByteEncryptor getByteEncryptor() {
        if (byteEncryptor == null) {
            throw new EmptyEncryptorException();
        }
        return byteEncryptor;
    }

    @Override
    public byte[] translate(byte[] data) {
        return this.getByteEncryptor().translate(data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.getSerializer().configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        // TODO extra checks
        return this.translate(this.getSerializer().serialize(topic, data));
    }

    @Override
    public void close() {
        // TODO
    }

}
