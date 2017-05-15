package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.errors.client.EmptyDecryptorException;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Combine Decorator and Strategy pattern responsibilities
 * Decorator in context if Deserializer
 * Strategy in context of different ByteDecryptor
 */
public class ByteDecryptorDeserializer<T> implements ByteDecryptor, Deserializer<T>, Configurable {

    private Deserializer<T> deserializer;
    private ByteDecryptor byteDecryptor;

    /**
     * Will use NonByteDecryptor
     *
     * @param deserializer -
     */
    public ByteDecryptorDeserializer(Deserializer<T> deserializer) {
        this.deserializer = deserializer;
        this.byteDecryptor = new NonByteDecryptor();
    }

    /**
     * @param deserializer
     * @param byteDecryptor
     */
    public ByteDecryptorDeserializer(Deserializer<T> deserializer, ByteDecryptor byteDecryptor) {
        this.deserializer = deserializer;
        this.byteDecryptor = byteDecryptor;
    }
    public Deserializer<T> getDeserializer() {
        return deserializer;
    }

    public ByteDecryptor getByteDecryptor() {
        if(byteDecryptor == null){
            throw new EmptyDecryptorException();
        }
        return byteDecryptor;
    }

    @Override
    public byte[] translate(byte[] data) {
        return this.getByteDecryptor().translate(data);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        // TODO need extra checks
        byte[] decrypted = this.translate(data);
        return this.getDeserializer().deserialize(topic, decrypted);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.getDeserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        // TODO need to implement
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO
    }
}
