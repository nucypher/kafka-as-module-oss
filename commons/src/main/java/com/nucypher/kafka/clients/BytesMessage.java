package com.nucypher.kafka.clients;

import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;

/**
 * Byte array message for re-encryption
 */
public class BytesMessage extends Message<byte[]> {

    /**
     * Constructor for re-encryption
     *
     * @param messageBytes -
     * @throws IOException            -
     * @throws ClassNotFoundException -
     */
    public BytesMessage(byte[] messageBytes) throws IOException, ClassNotFoundException {
        setMessageBytes(messageBytes);
        byte[] headerBytes = extractHeader(messageBytes);
        setHeader(new Header(headerBytes));
        byte[] payloadBytes = extractPayload(getMessageBytes());
        setPayload(payloadBytes);
    }

    /**
     * Constructor for encryption and serialization
     *
     * @param payloadEncryptor  -
     * @param header            -
     * @param payload           -
     */
    //TODO javadoc
    public BytesMessage(ByteEncryptor payloadEncryptor, Header header, byte[] payload)
            throws IOException, ClassNotFoundException {
        super(new ByteArraySerializer(), payloadEncryptor, header, payload);
    }



    /**
     * Constructor for decryption and deserialization
     *
     * @param payloadDecryptor -
     * @param messageBytes -
     * @throws IOException -
     * @throws ClassNotFoundException -
     */
    //TODO javadoc
    public BytesMessage(ByteDecryptor payloadDecryptor, byte[] messageBytes)
            throws IOException, ClassNotFoundException {
        super(new ByteArrayDeserializer(), payloadDecryptor, messageBytes);
    }

    /**
     * Serialize Message into byte array
     *
     * @return - byte[]
     * @throws IOException -
     */
    //TODO javadoc
    public byte[] serialize() throws IOException {
        byte[] headerBytes = getHeader().serialize();
        byte[] payloadBytes = getPayload();
        return combine(headerBytes, payloadBytes);
    }

}
