package com.nucypher.kafka.utils;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;

/**
 * Basically for test only
 */
public class ByteUtils {

    private static final BaseEncoding HEX = BaseEncoding.base16();

    public static final int BYTES_IN_LONG = 8;
    public static final int BYTES_IN_INT = 4;

    /**
     * Invert byte
     *
     * @return - inverted byte
     */
    public static byte invert(byte _byte) {
        return (byte) ~_byte;
    }

    /**
     * Invert byte array
     *
     * @param _array -
     * @return - inverted byte array
     */
    public static byte[] invert(byte[] _array) {
        if (_array == null) {
            return null;
        }
        byte[] _inv = new byte[_array.length];
        for (int i = 0; i < _array.length; i++) {
            _inv[i] = (byte) ~_array[i];
        }
        return _inv;
    }

    /**
     * byte[] to HEX String
     *
     * @param bytes - array of bytes byte[]
     * @return - HEX String representation
     */
    public static String hex(byte[] bytes) {
        return HEX.encode(bytes);
    }

    /**
     * Serialize any valid Object into byte[]
     *
     * @param value - any object
     * @return - byte[]
     */
    public static byte[] serialize(final Object value) {
        Schema schema;
        if (value == null) {
            schema = SchemaBuilder.builder().nullType();
        } else if (value instanceof GenericContainer) {
            schema = ((GenericContainer) value).getSchema();
        } else {
            schema = ReflectData.get().getSchema(value.getClass());
        }
        return AvroUtils.serialize(schema, value);
    }

    /**
     * Deserialize byte[] into Object
     *
     * @param bytes bytes
     * @return deserialized object
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        Schema schema;
        if (bytes == null) {
            schema = SchemaBuilder.builder().nullType();
        } else {
            schema = ReflectData.get().getSchema(clazz);
        }
        return (T) AvroUtils.deserialize(schema, bytes);
    }

    public static byte[] intToByteArray(int value) {
        return Ints.toByteArray(value);
    }

    public static int byteArrayToInt(byte[] bytes) {
        return Ints.fromByteArray(bytes);
    }

    public static byte[] longToByteArray(long value) {
        return Longs.toByteArray(value);
    }

    public static long byteArrayToLong(byte[] bytes) {
        return Longs.fromByteArray(bytes);
    }
}
