package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AvroUtils;
import com.nucypher.kafka.utils.GranularUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.GenericDataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * {@link StructuredDataAccessor} for Avro format
 */
//TODO simplify encrypted schema, move EDEKs field to Avro object
public class AvroDataAccessor extends AbstractAvroDataAccessor {

    private static final String ENCRYPTED_PROPERTY = "encrypted";
    private static final String EDEKS_PROPERTY = "edeks";

    //TODO change to external library
    protected Map<SchemaCacheKey, SchemaCacheValue> schemasCache = new HashMap<>();

    private GenericRecord currentRecord;
    private DataFileReader<GenericRecord> dataReader;
    private GenericDataFileWriter dataWriter;
    private ByteArrayOutputStream dataOutputStream;

    private MutableSchema mutableSchema;
    private Map<String, String> inputEncrypted;
    private Map<String, String> outputEncrypted;
    //TODO change EDEK value to bytes
    private Map<String, String> edeks;
    private Map<String, FieldObject> fieldsCache;

    private static class SchemaCacheKey {
        private Schema schema;
        private Set<String> fields;
        private Map<String, String> edeks;

        public SchemaCacheKey(Schema schema,
                              Set<String> fields,
                              Map<String, String> edeks) {
            this.schema = schema;
            this.fields = fields;
            this.edeks = edeks;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaCacheKey that = (SchemaCacheKey) o;
            return Objects.equals(schema, that.schema) &&
                    Objects.equals(fields, that.fields) &&
                    Objects.equals(edeks, that.edeks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, fields, edeks);
        }
    }

    private static class SchemaCacheValue {
        private Schema schema;
        private String schemaString;

        public SchemaCacheValue(Schema schema, String schemaString) {
            this.schema = schema;
            this.schemaString = schemaString;
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    @SuppressWarnings("unchecked")
    public void deserialize(String topic, byte[] data) {
        SeekableInput seekableInput = new SeekableByteArrayInput(data);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try {
            dataReader = new DataFileReader<>(seekableInput, datumReader);
        } catch (IOException e) {
            throw new CommonException(e);
        }
        Schema schema = dataReader.getSchema();
        currentRecord = null;
        dataWriter = null;
        dataOutputStream = null;

        Object encryptedProp = schema.getObjectProp(ENCRYPTED_PROPERTY);
        if (encryptedProp != null && !(encryptedProp instanceof Map)) {
            throw new CommonException(
                    "Property '%s' reserved for map of encrypted fields", ENCRYPTED_PROPERTY);
        }
        inputEncrypted = (Map<String, String>) encryptedProp;
        if (inputEncrypted == null) {
            inputEncrypted = new HashMap<>();
        }
        outputEncrypted = new HashMap<>(inputEncrypted);

        Object edeksProp = schema.getObjectProp(EDEKS_PROPERTY);
        if (edeksProp != null && !(edeksProp instanceof Map)) {
            throw new CommonException(
                    "Property '%s' reserved for EDEKs", EDEKS_PROPERTY);
        }
        edeks = (Map<String, String>) edeksProp;
        if (edeks == null) {
            edeks = new HashMap<>();
        }
        mutableSchema = new MutableSchema(schema, outputEncrypted, edeks);

        fieldsCache = new HashMap<>();
    }

    @Override
    public byte[] serialize() {
        if (currentRecord == null) {
            throw new CommonException("Current record is null");
        }
        if (dataOutputStream == null) {
            initializeOutput();
        }
        try {
            dataReader.close();
            dataWriter.append(currentRecord);
            dataWriter.close();
            return dataOutputStream.toByteArray();
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Get all fields which available for encryption.
     * The array indices and map keys are taken from the first record.
     *
     * @return collection of field names
     */
    @Override
    public Set<String> getAllFields() {
        Schema schema = dataReader.getSchema();
        seekToNext();
        Set<String> fields = getAllFields(
                new HashSet<String>(), schema, "", "", currentRecord);
        reset();
        return fields;
    }

    @Override
    public Map<String, byte[]> getAllEDEKs() {
        Map<String, byte[]> fields = new HashMap<>();
        if (edeks == null || edeks.isEmpty()) {
            return fields;
        }
        for (Map.Entry<String, String> entry : edeks.entrySet()) {
            String field = entry.getKey();
            String edek = entry.getValue();
            fields.put(field, DatatypeConverter.parseBase64Binary(edek));
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

        if (!outputEncrypted.containsKey(field)) {
            outputEncrypted.put(field, object.getInitialSchema().toString());
            object.updateSchema(Schema.create(Schema.Type.BYTES));
        }
    }

    @Override
    public void addEDEK(String field, byte[] edek) {
        edeks.put(field, DatatypeConverter.printBase64Binary(edek));
    }

    @Override
    public void addUnencrypted(String field, byte[] data) {
        FieldObject object = getFieldObject(field);
        String schemaString = inputEncrypted.get(field);
        Schema schema = new Schema.Parser().parse(schemaString);

        Object value = AvroUtils.deserialize(schema, data);
        object.setValue(value);

        if (outputEncrypted.containsKey(field)) {
            object.setSchema(schema);
            outputEncrypted.remove(field);
        }
    }

    @Override
    public void removeEDEK(String field) {
        edeks.remove(field);
    }

    @Override
    public boolean hasNext() {
        return dataReader.hasNext();
    }

    @Override
    public void seekToNext() throws NoSuchElementException {
        if (currentRecord != null && dataOutputStream == null) {
            initializeOutput();
        }
        try {
            if (currentRecord != null) {
                dataWriter.append(currentRecord);
            }
            currentRecord = dataReader.next(currentRecord);
            fieldsCache.clear();
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    private void initializeOutput() {
        dataOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
        dataWriter = new GenericDataFileWriter(writer);
        try {
            SchemaCacheValue value;
            SchemaCacheKey key = new SchemaCacheKey(
                    mutableSchema.getInitialSchema(),
                    outputEncrypted.keySet(),
                    edeks);
            if (schemasCache.containsKey(key)) {
                value = schemasCache.get(key);
            } else {
                Schema schema = mutableSchema.toSchema();
                value = new SchemaCacheValue(schema, schema.toString());
                schemasCache.put(key, value);
            }
            dataWriter.create(value.schema, value.schemaString, dataOutputStream);
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    @Override
    public void reset() {
        try {
            currentRecord = null;
            dataOutputStream = null;
            fieldsCache.clear();
            dataReader.sync(0);
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    @SuppressWarnings("all")
    private FieldObject getFieldObject(String path) {
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
        MutableSchema schema = mutableSchema;
        String parentPath;
        String currentPath = "";
        Integer index = null;
        String fieldName = null;
        for (int i = 0; i < pathList.size(); i++) {
            parent = currentObject;
            parentPath = currentPath;
            if (i > 0) {
                schema = schema.wrap(fieldName);
            }
            currentPath = GranularUtils.getFieldName(parentPath, pathList.get(i));
            switch (schema.getType()) {
                case RECORD:
                    fieldName = pathList.get(i);
                    currentObject = ((GenericRecord) parent).get(fieldName);
                    index = null;
                    break;
                case MAP:
                    fieldName = pathList.get(i);
                    Map<Utf8, Object> map = (Map<Utf8, Object>) parent;
                    currentObject = map.get(new Utf8(fieldName));//TODO can be string
                    index = null;
                    schema.setEncryptedItems(parentPath, inputEncrypted);
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
                    schema.setEncryptedItems(parentPath, inputEncrypted);
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
            Schema objectSchema = schema.getInitialChildSchema(fieldName);
            if (!checkNullable(objectSchema)) {
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
        private MutableSchema mutableSchema;

        public FieldObject(Object parent,
                           Object childObject,
                           Integer index,
                           String fieldName,
                           MutableSchema mutableSchema) {
            this.parent = parent;
            this.childObject = childObject;
            this.index = index;
            this.fieldName = fieldName;
            this.mutableSchema = mutableSchema;
        }

        public Object getValue() {
            return childObject;
        }

        public void setValue(Object object) {
            switch (mutableSchema.getType()) {
                case RECORD:
                    ((GenericRecord) parent).put(fieldName, object);
                    break;
                case MAP:
                    ((Map<Utf8, Object>) parent).put(new Utf8(fieldName), object);
                    break;
                case ARRAY:
                    ((List<Object>) parent).set(index, object);
                    break;
                default:
                    throw new CommonException(
                            "Unsupported schema type '%s'", mutableSchema.getType());

            }
        }

        public Schema getSchema() {
            return mutableSchema.getCurrentChildSchema(fieldName);
        }

        public Schema getInitialSchema() {
            return mutableSchema.getInitialChildSchema(fieldName);
        }

        public void updateSchema(Schema schema) {
            mutableSchema.updateInitialChildSchema(schema, fieldName);
        }

        public void setSchema(Schema schema) {
            mutableSchema.setInitialChildSchema(schema, fieldName);
        }
    }

    /**
     * Mutable {@link Schema} wrapper. Call {@link #toSchema()} to get the final {@link Schema}
     */
    private static class MutableSchema {
        private MutableSchema updatedChildSchema;
        private Integer encryptedItems;
        private Map<String, MutableSchema> updatedChildFields;
        private Schema schema;
        private Schema initialChildSchema;
        private Map<String, String> encrypted;
        private Map<String, String> edeks;

        public MutableSchema(Schema schema) {
            this.schema = schema;
        }

        public MutableSchema(Schema schema,
                             Map<String, String> encrypted,
                             Map<String, String> edeks) {
            this(schema);
            this.encrypted = encrypted;
            this.edeks = edeks;
        }

        /**
         * Wrap field using {@link MutableSchema} and return wrapper
         *
         * @param fieldName field name (only for {@link Schema.Type#RECORD} type)
         * @return wrapped schema
         */
        public MutableSchema wrap(String fieldName) {
            switch (schema.getType()) {
                case RECORD:
                    if (updatedChildFields == null) {
                        updatedChildFields = new HashMap<>();
                    }
                    Schema.Field field = schema.getField(fieldName);
                    MutableSchema mutableSchema = new MutableSchema(field.schema());
                    updatedChildFields.put(fieldName, mutableSchema);
                    return mutableSchema;
                case ARRAY:
                    if (updatedChildSchema != null) {
                        return updatedChildSchema;
                    }
                    updatedChildSchema = new MutableSchema(schema.getElementType());
                    return updatedChildSchema;
                case MAP:
                    if (updatedChildSchema != null) {
                        return updatedChildSchema;
                    }
                    updatedChildSchema = new MutableSchema(schema.getValueType());
                    return updatedChildSchema;
                default:
                    throw new CommonException("Unsupported schema type '%s'", schema.getType());
            }
        }

        /**
         * Add new schema to the child schema for {@link Schema.Type#ARRAY}
         * or {@link Schema.Type#MAP} types
         * or set child schema for {@link Schema.Type#RECORD} type
         *
         * @param schema    new schema
         * @param fieldName field name (only for {@link Schema.Type#RECORD} type)
         */
        public void updateInitialChildSchema(Schema schema, String fieldName) {
            if (encryptedItems != null && encryptedItems > 0) {
                return;
            }
            if (this.schema.getType() != Schema.Type.RECORD) { //MAP, ARRAY
                Schema oldSchema = getInitialChildSchema(fieldName);
                schema = addSchema(oldSchema, schema);
            }
            setChildSchema(schema, fieldName);
        }

        /**
         * Set child schema
         *
         * @param schema    new schema
         * @param fieldName field name (only for {@link Schema.Type#RECORD} type)
         */
        public void setInitialChildSchema(Schema schema, String fieldName) {
            if (encryptedItems == null || --encryptedItems == 0) {
                setChildSchema(schema, fieldName);
            }
        }

        private void setChildSchema(Schema schema, String fieldName) {
            switch (this.schema.getType()) {
                case RECORD:
                    if (updatedChildFields == null) {
                        updatedChildFields = new HashMap<>();
                    }
                    updatedChildFields.put(fieldName, new MutableSchema(schema));
                    break;
                case ARRAY:
                case MAP:
                    updatedChildSchema = new MutableSchema(schema);
                    break;
                default:
                    throw new CommonException(
                            "Unsupported schema type '%s'", this.schema.getType());
            }
        }

        /**
         * Get initial child schema
         *
         * @param fieldName field name (only for {@link Schema.Type#RECORD} type)
         * @return schema
         */
        public Schema getInitialChildSchema(String fieldName) {
            return getChildSchema(fieldName, true);
        }

        /**
         * Get current child schema
         *
         * @param fieldName field name (only for {@link Schema.Type#RECORD} type)
         * @return schema
         */
        public Schema getCurrentChildSchema(String fieldName) {
            return getChildSchema(fieldName, false);
        }

        private Schema getChildSchema(String fieldName, boolean initial) {
            switch (schema.getType()) {
                case RECORD:
                    Schema.Field field = schema.getField(fieldName);
                    if (field == null) {
                        return null;
                    }
                    return field.schema();
                case ARRAY:
                    if (initial && encryptedItems != null && encryptedItems > 0) {
                        return initialChildSchema;
                    }
                    return schema.getElementType();
                case MAP:
                    if (initial && encryptedItems != null && encryptedItems > 0) {
                        return initialChildSchema;
                    }
                    return schema.getValueType();
                default:
                    throw new CommonException("Unsupported schema type '%s'", schema.getType());
            }
        }

        /**
         * Apply all changes and construct final schema
         *
         * @return the final schema
         */
        @SuppressWarnings("unchecked")
        public Schema toSchema() {
            switch (schema.getType()) {
                case RECORD:
                    Map<String, String> oldEDEKs =
                        (Map<String, String>) schema.getObjectProp(EDEKS_PROPERTY);
                    if (updatedChildFields == null &&
                            (oldEDEKs == null && (edeks == null || edeks.isEmpty())) ||
                            (oldEDEKs != null && oldEDEKs.equals(edeks))) {
                        return schema;
                    }
                    return toRecordSchema();
                case ARRAY:
                    if (updatedChildSchema == null) {
                        return schema;
                    }
                    return Schema.createArray(updatedChildSchema.toSchema());
                case MAP:
                    if (updatedChildSchema == null) {
                        return schema;
                    }
                    return Schema.createMap(updatedChildSchema.toSchema());
                default:
                    return schema;
            }
        }

        private Schema toRecordSchema() {
            List<Schema.Field> fields = new ArrayList<>();
            for (Schema.Field field : schema.getFields()) {
                Schema.Field newField;
                if (updatedChildFields == null ||
                        !updatedChildFields.keySet().contains(field.name())) {
                    newField = new Schema.Field(
                            field.name(),
                            field.schema(),
                            field.doc(),
                            field.defaultVal(),
                            field.order());
                } else {
                    Schema fieldSchema = updatedChildFields.get(field.name()).toSchema();
                    newField = new Schema.Field(
                            field.name(),
                            fieldSchema,
                            field.doc(),
                            field.defaultVal());
                }
                fields.add(newField);
                for (String alias : field.aliases()) {
                    newField.addAlias(alias);
                }
            }

            Schema newSchema = Schema.createRecord(
                    schema.getName(),
                    schema.getDoc(),
                    schema.getNamespace(),
                    schema.isError(),
                    fields);
            for (Map.Entry<String, Object> property : schema.getObjectProps().entrySet()) {
                String propertyName = property.getKey();
                if (!propertyName.equals(ENCRYPTED_PROPERTY) &&
                        !propertyName.equals(EDEKS_PROPERTY)) {
                    newSchema.addProp(propertyName, property.getValue());
                }
            }
            if (encrypted != null && !encrypted.isEmpty()) {
                newSchema.addProp(ENCRYPTED_PROPERTY, encrypted);
            }
            if (edeks != null && !edeks.isEmpty()) {
                newSchema.addProp(EDEKS_PROPERTY, edeks);
            }
            for (String alias : schema.getAliases()) {
                newSchema.addAlias(alias);
            }
            return newSchema;
        }

        /**
         * @return schema type
         */
        public Schema.Type getType() {
            return schema.getType();
        }

        /**
         * Set number of encrypted child items (for {@link Schema.Type#MAP} or
         * {@link Schema.Type#ARRAY} type)
         *
         * @param path      parent path
         * @param encrypted encrypted fields
         */
        public void setEncryptedItems(String path, Map<String, String> encrypted) {
            if (encryptedItems != null) {
                return;
            }
            encryptedItems = 0;
            String itemPath = null;
            for (String fieldPath : encrypted.keySet()) {
                if (fieldPath.matches(path + "\\.[^.]+")) {
                    encryptedItems++;
                    itemPath = fieldPath;
                }
            }
            if (itemPath != null) {
                initialChildSchema = new Schema.Parser().parse(
                        encrypted.get(itemPath));
            }
        }

        /**
         * @return initial schema
         */
        public Schema getInitialSchema() {
            return schema;
        }
    }

}
