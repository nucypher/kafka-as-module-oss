package com.nucypher.kafka.utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;

/**
 * Basically for test only
 */
public class ByteUtils {

    /**
     * Serialize any valid Object into byte array
     *
     * @param value any object
     * @return byte array
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
     * Deserialize byte array into Object
     *
     * @param bytes bytes
     * @param clazz class of object
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

}
