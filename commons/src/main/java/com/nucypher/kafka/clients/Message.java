package com.nucypher.kafka.clients;

import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Objects;

/**
 * Message
 */
public class Message {

    private byte[] payload;
    private byte[] iv;
    private EncryptedDataEncryptionKey edek;

    /**
     * @param payload data
     * @param iv      IV bytes
     * @param edek    EDEK
     */
    public Message(byte[] payload, byte[] iv, EncryptedDataEncryptionKey edek) {
        this.payload = payload;
        this.edek = edek;
        this.iv = iv;
    }

    /**
     * @param payload data
     * @param iv      IV bytes
     * @param edek    EDEK bytes
     */
    public Message(byte[] payload, byte[] iv, byte[] edek) {
        this(payload, iv, new EncryptedDataEncryptionKey(edek));
    }

    /**
     * @param payload   data
     * @param iv        IV bytes
     * @param edek      EDEK bytes
     * @param isComplex is EDEK complex
     */
    public Message(byte[] payload, byte[] iv, byte[] edek, boolean isComplex) {
        this(payload, iv, new EncryptedDataEncryptionKey(edek, isComplex));
    }

    /**
     * @param payload data
     * @param iv      IV bytes
     */
    public Message(byte[] payload, byte[] iv) {
        this(payload, iv, (EncryptedDataEncryptionKey) null);
    }

    /**
     * @return data
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * @return EDEK
     */
    public EncryptedDataEncryptionKey getEDEK() {
        return edek;
    }

    /**
     * @param edek EDEK
     */
    public void setEDEK(EncryptedDataEncryptionKey edek) {
        this.edek = edek;
    }

    /**
     * @return IV
     */
    public byte[] getIV() {
        return iv;
    }

    /**
     * Serialize {@link Message}
     *
     * @return serialized data
     */
    public byte[] serialize() {
        int length = 8 + payload.length + iv.length + (edek != null ? edek.size() : 0);
        byte[] data = new byte[length];
        byte[] payloadLengthBytes = Ints.toByteArray(payload.length);
        byte[] ivLengthBytes = Ints.toByteArray(iv.length);
        System.arraycopy(payloadLengthBytes, 0, data, 0, payloadLengthBytes.length);
        System.arraycopy(payload, 0, data, 4, payload.length);
        System.arraycopy(
                ivLengthBytes, 0, data, 4 + payload.length, ivLengthBytes.length);
        System.arraycopy(iv, 0, data, 8 + payload.length, iv.length);
        if (edek != null) {
            data = edek.serialize(data, 8 + payload.length + iv.length);
        }
        return data;
    }

    /**
     * Deserialize {@link Message} from byte array
     *
     * @param bytes byte array
     * @return {@link Message} instance
     */
    public static Message deserialize(byte[] bytes) {
        byte[] payload = new byte[
                Ints.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3])];
        System.arraycopy(bytes, 4, payload, 0, payload.length);
        int i = 4 + payload.length;
        byte[] iv = new byte[
                Ints.fromBytes(bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3])];
        System.arraycopy(bytes, i + 4, iv, 0, iv.length);
        EncryptedDataEncryptionKey edek = null;
        if (8 + payload.length + iv.length < bytes.length) {
            edek = EncryptedDataEncryptionKey.deserialize(
                    bytes, 8 + payload.length + iv.length);
        }

        return new Message(payload, iv, edek);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Arrays.equals(payload, message.payload) &&
                Arrays.equals(iv, message.iv) &&
                Objects.equals(edek, message.edek);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, iv, edek);
    }
}
