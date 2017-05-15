package com.nucypher.kafka.utils;

import com.nucypher.kafka.errors.CommonException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Class for working with Avro format
 *
 * @author szotov
 */
public class AvroUtils {

    /**
     * Get object from bytes
     *
     * @param schema schema
     * @param data   byte array
     * @return deserialized object
     */
    public static Object deserialize(Schema schema, byte[] data) {
        return deserialize(schema, data, false);
    }


    /**
     * Get object from bytes
     *
     * @param schema   schema
     * @param data     byte array
     * @param useSpecificAvroReader use specific reader
     * @return deserialized object
     */
    public static Object deserialize(Schema schema, byte[] data, boolean useSpecificAvroReader) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        DatumReader<Object> reader;
        if (!useSpecificAvroReader) {
            reader = new GenericDatumReader<>(schema);
        } else {
            reader = new SpecificDatumReader<>(schema);
        }
        Object value;
        try {
            value = reader.read(null, decoder);
        } catch (IOException e) {
            throw new CommonException(e);
        }
        return value;
    }

    /**
     * Serialize object to bytes
     *
     * @param schema schema
     * @param object object
     * @return bytes
     */
    public static byte[] serialize(Schema schema, Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        try {
            writer.write(object, encoder);
            encoder.flush();
            output.flush();
        } catch (IOException e) {
            throw new CommonException(e);
        }

        return output.toByteArray();
    }

}
