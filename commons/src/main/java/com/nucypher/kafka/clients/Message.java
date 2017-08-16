package com.nucypher.kafka.clients;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

/**
 * Message
 */
public class Message {

    private static final byte IS_COMPLEX_TRUE = 1;
    private static final byte IS_COMPLEX_FALSE = 0;

    private byte[] payload;
    private String topic;
    private byte[] edek;
    private byte[] iv;
    private boolean isComplex;

    /**
     * @param payload   data
     * @param topic     topic
     * @param edek      EDEK bytes
     * @param iv        IV bytes
     * @param isComplex is EDEK complex
     */
    public Message(byte[] payload, String topic, byte[] edek, byte[] iv, boolean isComplex) {
        this.payload = payload;
        this.topic = topic;
        this.edek = edek;
        this.iv = iv;
        this.isComplex = isComplex;
    }

    /**
     * @param payload data
     * @param topic   topic
     * @param edek    EDEK bytes
     * @param iv      IV bytes
     */
    public Message(byte[] payload, String topic, byte[] edek, byte[] iv) {
        this(payload, topic, edek, iv, false);
    }

    /**
     * @return data
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * @return topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @return EDEK
     */
    public byte[] getEDEK() {
        return edek;
    }

    /**
     * @param edek EDEK
     */
    public void setEDEK(byte[] edek) {
        this.edek = edek;
    }

    /**
     * @return IV
     */
    public byte[] getIV() {
        return iv;
    }

    /**
     * @return is EDEK complex
     */
    public boolean isComplex() {
        return isComplex;
    }

    /**
     * @param isComplex is EDEK complex
     */
    public void setComplex(boolean isComplex) {
        this.isComplex = isComplex;
    }

    /**
     * Serialize {@link Message}
     *
     * @return serialized data
     */
    public byte[] serialize() {
        byte[] topicBytes = topic.getBytes();
        byte[] isComplexBytes = !isComplex ?
                new byte[]{IS_COMPLEX_FALSE} : new byte[]{IS_COMPLEX_TRUE};

        byte[] payloadLengthBytes = Ints.toByteArray(payload.length);
        byte[] topicLengthBytes = Ints.toByteArray(topicBytes.length);
        byte[] edekLengthBytes = Ints.toByteArray(edek.length);
        byte[] ivLengthBytes = Ints.toByteArray(iv.length);
        return Bytes.concat(payloadLengthBytes,
                topicLengthBytes,
                edekLengthBytes,
                ivLengthBytes,
                payload,
                topicBytes,
                edek,
                iv,
                isComplexBytes);
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
        byte[] topicBytes = new byte[
                Ints.fromBytes(bytes[4], bytes[5], bytes[6], bytes[7])];
        byte[] edek = new byte[
                Ints.fromBytes(bytes[8], bytes[9], bytes[10], bytes[11])];
        byte[] iv = new byte[
                Ints.fromBytes(bytes[12], bytes[13], bytes[14], bytes[15])];
        System.arraycopy(bytes, 16, payload, 0, payload.length);
        System.arraycopy(bytes, 16 + payload.length,
                topicBytes, 0, topicBytes.length);
        System.arraycopy(bytes, 16 + payload.length + topicBytes.length,
                edek, 0, edek.length);
        System.arraycopy(bytes, 16 + payload.length + topicBytes.length + edek.length,
                iv, 0, iv.length);
        byte isComplexByte = bytes[
                16 + payload.length + topicBytes.length + edek.length + iv.length];

        return new Message(payload, new String(topicBytes), edek, iv,
                isComplexByte == IS_COMPLEX_TRUE);
    }

}
