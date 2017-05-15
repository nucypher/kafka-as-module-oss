package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.Message;
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor;
import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.errors.CommonException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;

/**
 * P - payload
 */
public class AesMessageDecryptorDeserializer<P> extends ByteDecryptorDeserializer<P> implements ByteDecryptor, Deserializer<P> {

    public AesMessageDecryptorDeserializer(Deserializer<P> deserializer, ByteDecryptor byteDecryptor) {
        super(deserializer, byteDecryptor);
    }

    public AesMessageDecryptorDeserializer(Deserializer<P> deserializer, PrivateKey privateKey) {
        this(deserializer, new AesGcmCipherDecryptor(privateKey));
    }

    @Override
    public P deserialize(String topic, byte[] data) {
        try {
            return new Message<>(
                    this.getDeserializer(),
                    this.getByteDecryptor(),
                    data
            ).decrypt();
        } catch (IOException | ClassNotFoundException | NoSuchAlgorithmException |
                InvalidKeyException | NoSuchProviderException | InvalidKeySpecException ex) {
            throw new CommonException("Unable to decrypt message for topic:" + topic, ex);
        }
    }
}
