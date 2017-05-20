package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.utils.GranularUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for {@link StructuredDataAccessor} for Avro format
 */
public abstract class AbstractAvroDataAccessor extends OneMessageDataAccessor {

    /**
     * Get all fields which available for encryption
     *
     * @param fields          container for fields
     * @param schema          schema
     * @param parentFieldName parent field name
     * @param childName       child name
     * @param value           child value
     * @return container for fields
     */
    @SuppressWarnings("unchecked")
    protected static Set<String> getAllFields(Set<String> fields,
                                              Schema schema,
                                              String parentFieldName,
                                              String childName,
                                              Object value) {
        String fieldName = GranularUtils.getFieldName(parentFieldName, childName);
        switch (schema.getType()) {
            case RECORD:
                GenericRecord record = (GenericRecord) value;
                for (Schema.Field field : schema.getFields()) {
                    getAllFields(fields,
                            field.schema(),
                            fieldName,
                            field.name(),
                            record.get(field.name()));
                }
                break;
            case ARRAY:
                List<Object> list = (List<Object>) value;
                for (int i = 0; i < list.size(); i++) {
                    getAllFields(fields,
                            schema.getElementType(),
                            fieldName,
                            String.valueOf(i + 1),
                            list.get(i));
                }
                break;
            case MAP:
                Map<Utf8, Object> map = (Map<Utf8, Object>) value; //TODO can be string type
                for (Utf8 field : map.keySet()) {
                    getAllFields(fields,
                            schema.getValueType(),
                            fieldName,
                            field.toString(),
                            map.get(field));
                }
                break;
            default:
                fields.add(fieldName);
        }
        return fields;
    }

    /**
     * Convert byte buffer to byte array
     *
     * @param byteBuffer byte buffer
     * @return byte array
     */
    protected static byte[] toByteArray(ByteBuffer byteBuffer) {
        byte[] byteArray = new byte[byteBuffer.limit()];
        byteBuffer.rewind();
        byteBuffer.get(byteArray);
        byteBuffer.rewind();
        return byteArray;
    }

    /**
     * Checks whether the schema is nullable
     *
     * @param schema schema
     * @return result of checking
     */
    protected static boolean checkNullable(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.getType() == Schema.Type.NULL) {
            return true;
        }
        if (schema.getType() != Schema.Type.UNION) {
            return false;
        }
        for (Schema unionSchema : schema.getTypes()) {
            if (unionSchema.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add schema to initial schema
     *
     * @param initialSchema initial schema
     * @param addition      extra schema
     * @return union of schemas or initial schema if additional schema is equal initial
     */
    protected static Schema addSchema(Schema initialSchema, Schema addition) {
        List<Schema> schemas;
        Schema schema = initialSchema;
        if (initialSchema.getType() == Schema.Type.UNION) {
            schemas = new ArrayList<>(initialSchema.getTypes());
        } else {
            schemas = new ArrayList<>();
            schemas.add(initialSchema);
        }
        if (!schemas.contains(addition)) {
            schemas.add(addition);
        }
        if (schemas.size() > 1) {
            schema = Schema.createUnion(schemas);
        }
        return schema;
    }
}
