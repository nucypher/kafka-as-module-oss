package com.nucypher.kafka.clients;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Message
 */
public class Message implements Serializable {

    private Header header;
    private byte[] payload;

    /**
     * @param header  {@link Header} object
     * @param payload data
     */
    public Message(Header header, byte[] payload) {
        this.header = header;
        this.payload = payload;
    }

    /**
     * @return {@link Header} object
     */
    public Header getHeader() {
        return header;
    }

    /**
     * @param header {@link Header} object
     */
    public void setHeader(Header header) {
        this.header = header;
    }

    /**
     * @return data
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * @param payload data
     */
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * Serialize {@link Message}
     *
     * @return serialized data
     */
    public byte[] serialize() {
        byte[] serializedHeader = header.serialize();
        byte[] headerLengthBytes = Ints.toByteArray(serializedHeader.length);
        byte[] payloadLengthBytes = Ints.toByteArray(payload.length);
        return Bytes.concat(headerLengthBytes, payloadLengthBytes, serializedHeader, payload);
    }

    /**
     * Deserialize {@link Message} from byte array
     *
     * @param bytes byte array
     * @return {@link Message} instance
     */
    public static Message deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] serializedHeader = new byte[buffer.getInt()];
        byte[] payload = new byte[buffer.getInt()];
        buffer.get(serializedHeader);
        buffer.get(payload);
        Header header = Header.deserialize(serializedHeader);
        return new Message(header, payload);
    }

}
