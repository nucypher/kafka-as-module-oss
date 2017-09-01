package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AvroUtils;
import com.nucypher.kafka.utils.GranularUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@link StructuredDataAccessor} for Avro format by Confluent
 */
public class AvroSchemaLessDataAccessor extends AbstractAvroDataAccessor {

    private static final String EDEKS_FIELD = "edeks";
    private static final String INITIAL_SCHEMA_ID_PROPERTY = "initialSchemaId";
    private static final Schema BYTES_TYPE = SchemaBuilder.builder().bytesType();

    private SchemaRegistryClient schemaRegistry;
    private KafkaAvroDeserializer deserializer;
    private KafkaAvroSerializer serializer;
    private boolean isKey;
    //TODO change to external library
    private Map<SchemaCacheKey, Schema> schemasCache = new HashMap<>();

    private GenericRecord currentRecord;
    //TODO change EDEK value to bytes
    private Map<String, String> edeks;
    private Map<String, FieldObject> fieldsCache;
    private String topic;
    private Schema initialSchema;
    private Schema encryptedSchema;

    private static class SchemaCacheKey {
        private String subject;
        private Schema schema;

        public SchemaCacheKey(String subject, Schema schema) {
            this.subject = subject;
            this.schema = schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaCacheKey that = (SchemaCacheKey) o;
            return Objects.equals(subject, that.subject) &&
                    Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subject, schema);
        }
    }

    public AvroSchemaLessDataAccessor() {

    }

    /**
     * @param schemaRegistry Schema Registry client
     */
    public AvroSchemaLessDataAccessor(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (schemaRegistry == null) {
            AbstractKafkaAvroSerDeConfig config = new AbstractKafkaAvroSerDeConfig(
                    AbstractKafkaAvroSerDeConfig.baseConfigDef(), configs);
            List<String> urls = config.getSchemaRegistryUrls();
            int maxSchemaObject = config.getMaxSchemasPerSubject();

            schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject);
        }
        deserializer = new KafkaAvroDeserializer(schemaRegistry);
        deserializer.configure(configs, isKey);
        serializer = new KafkaAvroSerializer(schemaRegistry);
        serializer.configure(configs, isKey);

        this.isKey = isKey;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void deserialize(String topic, byte[] data) {
        this.topic = topic;
        currentRecord = (GenericRecord) deserializer.deserialize(topic, data);
        Schema schema = currentRecord.getSchema();
        Object initialSchemaId = schema.getObjectProp(INITIAL_SCHEMA_ID_PROPERTY);
        if (initialSchemaId != null && !(initialSchemaId instanceof Integer)) {
            throw new CommonException(
                    "Property '%s' reserved for initial schema id", INITIAL_SCHEMA_ID_PROPERTY);
        }
        edeks = new HashMap<>();
        if (initialSchemaId != null) {
            encryptedSchema = schema;
            initialSchema = getInitialSchema((Integer) initialSchemaId);
            for (Map.Entry<?, ?> entry :
                    ((Map<String, ?>) currentRecord.get(EDEKS_FIELD)).entrySet()) {
                Object value = entry.getValue();
                edeks.put(entry.getKey().toString(), value != null ? value.toString() : "");
            }
        } else {
            initialSchema = schema;
            encryptedSchema = getEncryptedSchema(topic, schema);
        }
        fieldsCache = new HashMap<>();
        setEmpty(false);
    }

    private Schema getEncryptedSchema(String topic, Schema schema) {
        String subject = getSubjectName(topic, isKey);
        SchemaCacheKey pair = new SchemaCacheKey(subject, schema);
        if (schemasCache.containsKey(pair)) {
            return schemasCache.get(pair);
        }

        int initialSchemaId;
        try {
            initialSchemaId = schemaRegistry.register(subject, schema);
        } catch (IOException | RestClientException e) {
            throw new CommonException(e);
        }

        Schema encryptedSchema = getEncryptedSchema(schema, false);
        encryptedSchema.addProp(INITIAL_SCHEMA_ID_PROPERTY, initialSchemaId);
        schemasCache.put(pair, encryptedSchema);
        return encryptedSchema;
    }

    private Schema getEncryptedSchema(Schema schema, boolean addBytes) {
        Schema encryptedSchema;
        switch (schema.getType()) {
            case RECORD:
                List<Schema.Field> fields = new ArrayList<>();
                for (Schema.Field field : schema.getFields()) {
                    Schema.Field newField = new Schema.Field(
                            field.name(),
                            getEncryptedSchema(field.schema(), true),
                            field.doc(),
                            field.defaultVal(),
                            field.order());
                    fields.add(newField);
                    for (String alias : field.aliases()) {
                        newField.addAlias(alias);
                    }
                }
                if (!addBytes) {
                    Schema.Field newField = new Schema.Field(
                            EDEKS_FIELD,
                            SchemaBuilder.unionOf().nullType().and().map().values().stringType().endUnion(),
                            null,
                            JsonProperties.NULL_VALUE);
                    fields.add(newField);
                }

                Schema newSchema = Schema.createRecord(
                        schema.getName(),
                        schema.getDoc(),
                        schema.getNamespace(),
                        schema.isError(),
                        fields);
                for (Map.Entry<String, Object> property : schema.getObjectProps().entrySet()) {
                    String propertyName = property.getKey();
                    newSchema.addProp(propertyName, property.getValue());
                }
                for (String alias : schema.getAliases()) {
                    newSchema.addAlias(alias);
                }
                encryptedSchema = newSchema;
                break;
            case ARRAY:
                encryptedSchema = Schema.createArray(
                        getEncryptedSchema(schema.getElementType(), true));
                break;
            case MAP:
                encryptedSchema = Schema.createMap(
                        getEncryptedSchema(schema.getValueType(), true));
                break;
            default:
                encryptedSchema = schema;
                break;
        }
        if (addBytes) {
            return addSchema(encryptedSchema, BYTES_TYPE);
        }
        return encryptedSchema;
    }

    private Schema getInitialSchema(Integer initialSchemaId) {
        try {
            return schemaRegistry.getByID(initialSchemaId);
        } catch (IOException | RestClientException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Get the subject name for the given topic and value type.
     * Copied from {@link AbstractKafkaAvroSerDe}
     */
    private String getSubjectName(String topic, boolean isKey) {
        if (isKey) {
            return topic + "-key";
        } else {
            return topic + "-value";
        }
    }

    @Override
    public byte[] serialize() {
        if (currentRecord == null) {
            throw new CommonException("Current record is null");
        }
        GenericRecord record;
        if (edeks != null && !edeks.isEmpty()) {
            record = updateSchema(currentRecord, encryptedSchema);
            record.put(EDEKS_FIELD, edeks);
        } else {
            record = updateSchema(currentRecord, initialSchema);
        }
        return serializer.serialize(topic, record);
    }

    private GenericRecord updateSchema(GenericRecord record, Schema schema) {
        return new RecordWrapper(record, schema);
    }

    /**
     * Get all fields which available for encryption.
     * The array indices and map keys are taken from the first record.
     *
     * @return collection of field names
     */
    @Override
    public Set<String> getAllFields() {
        return getAllFields(
                new HashSet<String>(), initialSchema, "", "", currentRecord);
    }

    @Override
    public Map<String, byte[]> getAllEDEKs() {
        Map<String, byte[]> fields = new HashMap<>();
        if (edeks == null || edeks.isEmpty()) {
            return fields;
        }
        for (Map.Entry<String, String> entry : edeks.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            String field = entry.getKey();
            fields.put(field, DatatypeConverter.parseBase64Binary(entry.getValue()));
        }
        return fields;
    }

    @Override
    public byte[] getEncrypted(String field) {
        FieldObject object = getFieldObject(field);
        return toByteArray((ByteBuffer) object.getValue());
    }

    @Override
    public byte[] getUnencrypted(String field) {
        FieldObject object = getFieldObject(field);
        return AvroUtils.serialize(object.getSchema(), object.getValue());
    }

    @Override
    public void addEncrypted(String field, byte[] data) {
        FieldObject object = getFieldObject(field);
        object.setValue(ByteBuffer.wrap(data));
        if (!edeks.containsKey(field)) {
            edeks.put(field, "");
        }
    }

    @Override
    public void addEDEK(String field, byte[] edek) {
        edeks.put(field, DatatypeConverter.printBase64Binary(edek));
    }

    @Override
    public void addUnencrypted(String field, byte[] data) {
        FieldObject object = getFieldObject(field);
        Object value = AvroUtils.deserialize(object.getSchema(), data);
        object.setValue(value);
        edeks.remove(field);
    }

    @Override
    public void removeEDEK(String field) {
        if (edeks.containsKey(field)) {
            edeks.put(field, "");
        }
    }

    @SuppressWarnings("all")
    private FieldObject getFieldObject(String path) { //TODO refactor
        List<String> pathList = GranularUtils.parsePath(path);

        if (pathList == null || pathList.isEmpty()) {
            throw new CommonException("Input path is empty");
        }
        if (currentRecord == null) {
            throw new CommonException("Data accessor is not initialized");
        }
        if (fieldsCache.containsKey(path)) {
            return fieldsCache.get(path);
        }

        Object parent = currentRecord;
        Object currentObject = parent;
        Schema schema = initialSchema;
        String parentPath;
        String currentPath = "";
        Integer index = null;
        String fieldName = null;
        for (int i = 0; i < pathList.size(); i++) {
            parent = currentObject;
            parentPath = currentPath;
            currentPath = GranularUtils.getFieldName(parentPath, pathList.get(i));
            switch (schema.getType()) {
                case RECORD:
                    fieldName = pathList.get(i);
                    currentObject = ((GenericRecord) parent).get(fieldName);
                    Schema.Field field = schema.getField(fieldName);
                    if (field != null) {
                        schema = field.schema();
                    } else {
                        schema = null;
                    }
                    index = null;
                    break;
                case MAP:
                    fieldName = pathList.get(i);
                    Map<Utf8, Object> map = (Map<Utf8, Object>) parent;
                    currentObject = map.get(new Utf8(fieldName));//TODO can be string
                    index = null;
                    schema = schema.getValueType();
                    break;
                case ARRAY:
                    index = Integer.valueOf(pathList.get(i)) - 1; //TODO add checking
                    List<Object> list = (List<Object>) parent;
                    int fieldsCount = list.size();
                    if (fieldsCount - 1 < index) {
                        throw new CommonException(
                                "Field '%s' contains only '%d' elements but need '%d'",
                                parentPath, fieldsCount, index + 1);
                    }
                    currentObject = list.get(index);
                    fieldName = null;
                    schema = schema.getElementType();
                    break;
                default:
                    throw new CommonException(
                            "Field '%s' is neither record nor array nor map", parentPath);
            }
            if (currentObject != null) {
                continue;
            }
            CommonException exception = new CommonException("Field '%s' not found", currentPath);
            if (i < pathList.size() - 1) {
                throw exception;
            }
            if (!checkNullable(schema)) {
                throw exception;
            }
        }

        FieldObject object = new FieldObject(parent, currentObject, index, fieldName, schema);
        fieldsCache.put(path, object);
        return object;
    }

    @SuppressWarnings("unchecked")
    private static class FieldObject {
        private Object parent;
        private Object childObject;
        private Integer index;
        private String fieldName;
        private Schema childSchema;

        public FieldObject(Object parent,
                           Object childObject,
                           Integer index,
                           String fieldName,
                           Schema childSchema) {
            this.parent = parent;
            this.childObject = childObject;
            this.index = index;
            this.fieldName = fieldName;
            this.childSchema = childSchema;
        }

        public Object getValue() {
            return childObject;
        }

        public void setValue(Object object) {
            if (parent instanceof GenericRecord) {
                ((GenericRecord) parent).put(fieldName, object);
            } else if (parent instanceof Map) {
                ((Map<Utf8, Object>) parent).put(new Utf8(fieldName), object);
            } else if (parent instanceof List) {
                ((List<Object>) parent).set(index, object);
            } else {
                throw new CommonException(
                        "Unsupported object type '%s'", parent.getClass());
            }
        }

        public Schema getSchema() {
            return childSchema;
        }
    }

    private static class RecordWrapper implements GenericRecord {
        private GenericRecord record;
        private Schema schema;
        private Object encrypted;
        private Integer encryptedIndex;

        public RecordWrapper(GenericRecord record, Schema schema) {
            this.schema = schema;
            this.record = record;
            for (int i = 0; i < schema.getFields().size(); i++) {
                Schema.Field field = schema.getFields().get(i);
                if (field.name().equals(EDEKS_FIELD)) {
                    encryptedIndex = i;
                }
            }
            if (encryptedIndex == null) {
                encryptedIndex = schema.getFields().size();
            }
        }

        @Override
        public void put(String key, Object v) {
            if (EDEKS_FIELD.equals(key)) {
                encrypted = v;
            } else {
                record.put(key, v);
            }
        }

        @Override
        public Object get(String key) {
            return record.get(key);
        }

        @Override
        public void put(int i, Object v) {
            if (i == encryptedIndex) {
                encrypted = v;
            } else {
                record.put(i, v);
            }
        }

        @Override
        public Object get(int i) {
            if (i == encryptedIndex) {
                return encrypted;
            }
            return record.get(i);
        }

        @Override
        public Schema getSchema() {
            return schema;
        }
    }

}
