package com.nucypher.kafka.clients;

import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.Objects;

/**
 * Encrypted Data Encryption Key (EDEK)
 */
public class EncryptedDataEncryptionKey {

    private static final byte IS_COMPLEX_TRUE = 1;
    private static final byte IS_COMPLEX_FALSE = 0;

    private byte[] bytes;
    private boolean isComplex;

    /**
     * @param bytes     EDEK bytes
     * @param isComplex is EDEK complex
     */
    public EncryptedDataEncryptionKey(byte[] bytes, boolean isComplex) {
        this.bytes = bytes;
        this.isComplex = isComplex;
    }

    /**
     * @param bytes EDEK bytes
     */
    public EncryptedDataEncryptionKey(byte[] bytes) {
        this(bytes, false);
    }

    /**
     * @return EDEK bytes
     */
    public byte[] getBytes() {
        return bytes;
    }


    /**
     * @return is EDEK complex
     */
    public boolean isComplex() {
        return isComplex;
    }

    /**
     * @return size of serialized EDEK
     */
    public int size() {
        return bytes.length + 5;
    }

    /**
     * Serialize {@link EncryptedDataEncryptionKey}
     *
     * @return serialized data
     */
    public byte[] serialize() {
        return serialize(new byte[size()], 0);
    }

    /**
     * Serialize {@link EncryptedDataEncryptionKey} into buffer
     *
     * @param buffer buffer byte array
     * @param offset start offset
     * @return serialized data
     */
    public byte[] serialize(byte[] buffer, int offset) {
        buffer[offset] = !isComplex ? IS_COMPLEX_FALSE : IS_COMPLEX_TRUE;
        byte[] edekLengthBytes = Ints.toByteArray(bytes.length);
        System.arraycopy(
                edekLengthBytes, 0, buffer, offset + 1, edekLengthBytes.length);
        System.arraycopy(
                bytes, 0, buffer, offset + edekLengthBytes.length + 1, bytes.length);
        return buffer;
    }

    /**
     * Deserialize {@link EncryptedDataEncryptionKey} from byte array
     *
     * @param bytes byte array
     * @return {@link EncryptedDataEncryptionKey} instance
     */
    public static EncryptedDataEncryptionKey deserialize(byte[] bytes) {
        return deserialize(bytes, 0);
    }

    /**
     * Deserialize {@link EncryptedDataEncryptionKey} from byte array
     *
     * @param bytes  byte array
     * @param offset start offset
     * @return {@link EncryptedDataEncryptionKey} instance
     */
    public static EncryptedDataEncryptionKey deserialize(byte[] bytes, int offset) {
        byte isComplexByte = bytes[offset];
        byte[] edek = new byte[Ints.fromBytes(
                bytes[offset + 1], bytes[offset + 2], bytes[offset + 3], bytes[offset + 4])];
        System.arraycopy(bytes, offset + 5, edek, 0, edek.length);
        return new EncryptedDataEncryptionKey(edek, isComplexByte == IS_COMPLEX_TRUE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncryptedDataEncryptionKey that = (EncryptedDataEncryptionKey) o;
        return isComplex == that.isComplex &&
                Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytes, isComplex);
    }
}
