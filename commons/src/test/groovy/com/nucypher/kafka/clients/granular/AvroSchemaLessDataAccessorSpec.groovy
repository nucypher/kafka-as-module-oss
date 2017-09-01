package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.AvroUtils
import com.nucypher.kafka.utils.ByteUtils
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer

/**
 * Test for {@link AvroSchemaLessDataAccessor}
 */
class AvroSchemaLessDataAccessorSpec extends Specification {

    @Shared
    Schema schema = AvroTestUtils.getSchema()
    @Shared
    Schema encryptedSchema
    @Shared
    GenericRecord firstRecord = AvroTestUtils.getFirstRecord()
    @Shared
    GenericRecord secondRecord = AvroTestUtils.getSecondRecord()
    @Shared
    GenericRecord encryptedFirstRecord
    @Shared
    GenericRecord encryptedSecondRecord
    @Shared
    GenericRecord partiallyEncryptedFirstRecord
    @Shared
    GenericRecord partiallyEncryptedFirstNoEDEKsRecord
    @Shared
    GenericRecord partiallyEncryptedSecondRecord
    @Shared
    GenericRecord partiallyEncryptedSecondNoEDEKsRecord
    @Shared
    String topic = "topic"
    @Shared
    int schemaId = 1
    @Shared
    int encryptedSchemaId = 2

    AvroSchemaLessDataAccessor dataAccessor

    def setupSpec() {
        Schema enumSchema = AvroTestUtils.getEnumSchema()
        Schema fixedSchema = AvroTestUtils.getFixedSchema()

        Schema encryptedInnerRecordSchema = SchemaBuilder.record("record1").fields()
                .name("int").type().unionOf().intType()
                .and().bytesType().endUnion().noDefault().endRecord()
        Schema encryptedComplexInnerRecordSchema = SchemaBuilder.record("record2").fields()
                .name("record").type(
                SchemaBuilder.unionOf()
                        .type(encryptedInnerRecordSchema).and().bytesType().endUnion())
                .noDefault().endRecord()

        encryptedSchema = SchemaBuilder.record("record").namespace("namespace")
                .fields()
                .name("int").aliases("int_alias").doc("doc").type().unionOf().intType()
                .and().bytesType().endUnion().intDefault(1)
                .name("unencrypted").type().unionOf().intType()
                .and().bytesType().endUnion().intDefault(1)
                .name("boolean").type().unionOf().booleanType()
                .and().bytesType().endUnion().noDefault()
                .name("bytes").type().bytesType().noDefault()
                .name("double").type().unionOf().doubleType()
                .and().bytesType().endUnion().noDefault()
                .name("float").type().unionOf().floatType()
                .and().bytesType().endUnion().noDefault()
                .name("long").type().unionOf().longType()
                .and().bytesType().endUnion().noDefault()
                .name("string").type().unionOf().stringType()
                .and().bytesType().endUnion().noDefault()
                .name("null").type().unionOf().nullType()
                .and().bytesType().endUnion().nullDefault()
                .name("null2").type().unionOf().nullType().and().intType()
                .and().bytesType().endUnion().nullDefault()
                .name("enum").type(SchemaBuilder.unionOf().type(enumSchema)
                .and().bytesType().endUnion()).noDefault()
                .name("fixed").type(SchemaBuilder.unionOf().type(fixedSchema)
                .and().bytesType().endUnion()).noDefault()
                .name("record").type(SchemaBuilder.unionOf().type(encryptedInnerRecordSchema)
                .and().bytesType().endUnion()).noDefault()
                .name("map").type().unionOf().map().values().unionOf().intType()
                .and().bytesType().endUnion().and().bytesType().endUnion().noDefault()
                .name("array").type().unionOf().array().items().bytesType()
                .and().bytesType().endUnion().noDefault()
                .name("union").type().unionOf().intType().and().stringType()
                .and().bytesType().endUnion().noDefault()
                .name("union2").type().unionOf().map().values().intType().and().intType()
                .and().bytesType().endUnion().noDefault()
                .name("complex").type(SchemaBuilder.unionOf().type(encryptedComplexInnerRecordSchema)
                .and().bytesType().endUnion()).noDefault()
                .name("edeks").type().optional().map().values().stringType()
                .endRecord()
        encryptedSchema.addProp("initialSchemaId", schemaId)

        Map<String, String> edeksMap = new HashMap<>()
        AvroTestUtils.addEDEKs(edeksMap, "string", "array.2", "array.1", "union2",
                "double", "union", "float", "int", "long", "enum", "record.int", "null2",
                "map.key2", "map.key1", "boolean", "null", "bytes", "fixed", "complex.record")
        Map<Utf8, Utf8> edeks = new HashMap<>()
        for (Map.Entry<String, String> entry : edeksMap.entrySet()) {
            edeks.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()))
        }
        edeksMap = new HashMap<>()
        AvroTestUtils.addEDEKs(edeksMap, "map.key1", "array.1", "boolean")
        Map<Utf8, Utf8> partiallyEDEKs = new HashMap<>()
        for (Map.Entry<String, String> entry : edeksMap.entrySet()) {
            partiallyEDEKs.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()))
        }
        Map<Utf8, Utf8> partiallyNoEDEKs = new HashMap<>()
        for (Map.Entry<String, String> entry : edeksMap.entrySet()) {
            partiallyNoEDEKs.put(new Utf8(entry.getKey()), new Utf8(""))
        }

        Map<Utf8, Integer> map = firstRecord.get("map") as Map<Utf8, Integer>
        List<ByteBuffer> list = firstRecord.get("array") as List<ByteBuffer>
        GenericRecord innerRecord = firstRecord.get("record") as GenericRecord
        GenericRecord complexInnerRecord = firstRecord.get("complex") as GenericRecord

        encryptedFirstRecord = new GenericData.Record(encryptedSchema)
        encryptedFirstRecord.put("unencrypted", 3)
        AvroTestUtils.encryptFields(firstRecord, encryptedFirstRecord,
                "int", "boolean", "bytes", "double", "float", "long", "string",
                "null", "null2", "enum", "fixed", "union", "union2")
        GenericRecord encryptedInnerRecord = new GenericData.Record(encryptedInnerRecordSchema)
        AvroTestUtils.encryptFields(innerRecord, encryptedInnerRecord, "int")
        encryptedFirstRecord.put("record", encryptedInnerRecord)
        Map<Utf8, Object> encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1", "key2")
        encryptedFirstRecord.put("map", encryptedMap)
        List<Object> encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0, 1)
        encryptedFirstRecord.put("array", encryptedList)
        GenericRecord encryptedComplexInnerRecord = new GenericData.Record(encryptedComplexInnerRecordSchema)
        AvroTestUtils.encryptFields(complexInnerRecord, encryptedComplexInnerRecord, "record")
        encryptedFirstRecord.put("complex", encryptedComplexInnerRecord)
        encryptedFirstRecord.put("edeks", edeks)

        partiallyEncryptedFirstNoEDEKsRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(firstRecord, partiallyEncryptedFirstNoEDEKsRecord)
        AvroTestUtils.encryptFields(firstRecord, partiallyEncryptedFirstNoEDEKsRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedFirstNoEDEKsRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedFirstNoEDEKsRecord.put("array", encryptedList)
        partiallyEncryptedFirstNoEDEKsRecord.put("edeks", partiallyNoEDEKs)

        partiallyEncryptedFirstRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(partiallyEncryptedFirstNoEDEKsRecord, partiallyEncryptedFirstRecord)
        partiallyEncryptedFirstRecord.put("edeks", partiallyEDEKs)

        map = secondRecord.get("map") as Map<Utf8, Integer>
        list = secondRecord.get("array") as List<ByteBuffer>
        innerRecord = secondRecord.get("record") as GenericRecord
        complexInnerRecord = secondRecord.get("complex") as GenericRecord

        encryptedSecondRecord = new GenericData.Record(encryptedSchema)
        encryptedSecondRecord.put("unencrypted", 4)
        AvroTestUtils.encryptFields(secondRecord, encryptedSecondRecord,
                "int", "boolean", "bytes", "double", "float", "long", "string",
                "null", "null2", "enum", "fixed", "union", "union2")
        encryptedInnerRecord = new GenericData.Record(encryptedInnerRecordSchema)
        AvroTestUtils.encryptFields(innerRecord, encryptedInnerRecord, "int")
        encryptedSecondRecord.put("record", encryptedInnerRecord)
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1", "key2")
        encryptedSecondRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0, 1)
        encryptedSecondRecord.put("array", encryptedList)
        encryptedComplexInnerRecord = new GenericData.Record(encryptedComplexInnerRecordSchema)
        AvroTestUtils.encryptFields(complexInnerRecord, encryptedComplexInnerRecord, "record")
        encryptedSecondRecord.put("complex", encryptedComplexInnerRecord)
        encryptedSecondRecord.put("edeks", edeks)

        partiallyEncryptedSecondNoEDEKsRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(secondRecord, partiallyEncryptedSecondNoEDEKsRecord)
        AvroTestUtils.encryptFields(secondRecord, partiallyEncryptedSecondNoEDEKsRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedSecondNoEDEKsRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedSecondNoEDEKsRecord.put("array", encryptedList)
        partiallyEncryptedSecondNoEDEKsRecord.put("edeks", partiallyNoEDEKs)

        partiallyEncryptedSecondRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(partiallyEncryptedSecondNoEDEKsRecord, partiallyEncryptedSecondRecord)
        partiallyEncryptedSecondRecord.put("edeks", partiallyEDEKs)
    }

    def setup() {
        Map<String, ?> configs = new HashMap<>()
        configs.put("schema.registry.url", "url")
        SchemaRegistryClient schemaRegistry = Mock()
        schemaRegistry.getBySubjectAndID(null, encryptedSchemaId) >> encryptedSchema
        schemaRegistry.getBySubjectAndID(null, schemaId) >> schema
        schemaRegistry.register(_, encryptedSchema) >> encryptedSchemaId
        schemaRegistry.register(_, schema) >> schemaId
        schemaRegistry.getByID(schemaId) >> schema
        dataAccessor = new AvroSchemaLessDataAccessor(schemaRegistry)
        dataAccessor.configure(configs, false)
    }

    def 'getting next element'() {
        setup: 'initialization'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, record)

        when: 'deserialize one record'
        dataAccessor.deserialize(topic, data)

        then: 'accessor has one element'
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        !dataAccessor.hasNext()

        when: 'seek to the next element'
        dataAccessor.seekToNext()

        then: 'thrown NoSuchElementException exception'
        thrown(NoSuchElementException)

        when: 'reset accessor'
        dataAccessor.reset()

        then: 'accessor has one element'
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        !dataAccessor.hasNext()

        where:
        record << [firstRecord, secondRecord]
    }

    def 'serialization and deserialization'() {
        when: 'deserialize and serialize Avro bytes'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                inputSchema, inputSchemaId, inputRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.IdRecord idRecord =
                AvroTestUtils.deserializeWithoutSchema(inputSchema, bytes)

        then: 'should be the same data'
        idRecord.id == inputSchemaId
        inputRecord.equals(idRecord.genericRecord)

        where:
        inputSchemaId << [
                schemaId,
                schemaId,
                encryptedSchemaId,
                encryptedSchemaId,
                encryptedSchemaId,
                encryptedSchemaId
        ]
        inputSchema << [
                schema,
                schema,
                encryptedSchema,
                encryptedSchema,
                encryptedSchema,
                encryptedSchema
        ]
        inputRecord << [
                firstRecord,
                secondRecord,
                encryptedFirstRecord,
                encryptedSecondRecord,
                partiallyEncryptedFirstRecord,
                partiallyEncryptedSecondRecord
        ]
    }

    def 'getting all fields'() {
        when: 'get all fields from record'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, inputRecord)
        dataAccessor.deserialize(topic, data)

        then: 'should be all fields'
        dataAccessor.getAllFields() ==
                ["int", "boolean", "bytes", "double", "float",
                 "long", "string", "null", "null2", "enum", "fixed",
                 "record.int", "map.key1", "map.key2", "array.1",
                 "array.2", "union", "union2", "complex.record.int",
                 "unencrypted"].toSet()

        where:
        inputRecord << [firstRecord, secondRecord]
    }

    def 'getting value by field name with simple types'() {
        setup: 'initialization'
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "enum", "fixed",
                                   "null2", "union", "union2"]

        when: 'get value of field from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        then: 'should be right value'
        fieldNames.every {
            byte[] unencryptedField = dataAccessor.getUnencrypted(it)
            unencryptedField == AvroUtils.serialize(
                    schema.getField(it).schema(), record.get(it))
        }

        where:
        record << [firstRecord, secondRecord]
    }

    def 'getting value by field name with complex types'() {
        when: 'get value of field from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        then: 'should be right value'
        dataAccessor.getUnencrypted("record.int") ==
                ByteUtils.serialize(((GenericRecord) record.get("record")).get("int"))
        dataAccessor.getUnencrypted("map.key1") ==
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1")))
        dataAccessor.getUnencrypted("map.key2") ==
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2")))
        dataAccessor.getUnencrypted("array.1") ==
                ByteUtils.serialize(((List<Object>) record.get("array")).get(0))
        dataAccessor.getUnencrypted("array.2") ==
                ByteUtils.serialize(((List<Object>) record.get("array")).get(1))
        dataAccessor.getUnencrypted("complex.record") ==
                ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record"))

        where:
        record << [firstRecord, secondRecord]
    }

    def 'getting value by nonexistent field'() {
        setup: 'initialization'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        when: 'get value of nonexistent field'
        dataAccessor.getUnencrypted(fieldName)

        then: 'thrown not-found exception'
        thrown(CommonException)

        where:
        fieldName << ["test", "record.int2", "array.3", "map.key3", "union3.record"]
    }

    def 'getting encrypted values with simple types'() {
        setup: 'initialization'
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "null2", "enum", "fixed",
                                   "union", "union2"]

        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        then: 'should be right values'
        fieldNames.every {
            byte[] encryptedField = dataAccessor.getEncrypted(it)
            encryptedField == ((ByteBuffer) record.get(it)).array()
        }

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting encrypted values with complex types'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        then: 'should be right values'
        dataAccessor.getEncrypted("record.int") == ((ByteBuffer)
                ((GenericRecord) record.get("record")).get("int")).array()
        dataAccessor.getEncrypted("map.key1") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
        dataAccessor.getEncrypted("map.key2") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))).array()
        dataAccessor.getEncrypted("array.1") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(0)).array()
        dataAccessor.getEncrypted("array.2") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(1)).array()
        dataAccessor.getEncrypted("complex.record") == ((ByteBuffer)
                ((GenericRecord) record.get("complex")).get("record")).array()

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting encrypted value by nonexistent field'() {
        setup: 'initialization'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        when: 'get value of nonexistent field'
        dataAccessor.getEncrypted(fieldName)

        then: 'thrown not-found exception'
        thrown(CommonException)

        where:
        fieldName << ["test", "record.int2", "array.3", "map.key3", "union3.record"]
    }

    def 'getting all encrypted values from partially encrypted record'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        then: 'should be right values'
        dataAccessor.getEncrypted("boolean") == ((ByteBuffer) record.get("boolean")).array()
        dataAccessor.getEncrypted("map.key1") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
        dataAccessor.getEncrypted("array.1") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(0)).array()

        where:
        record << [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
    }

    def 'getting all EDEKs with simple types'() {
        setup: 'initialization'
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "null2", "enum", "fixed",
                                   "union", "union2"]

        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be right values'
        values.size() == 19
        fieldNames.every {
            new String(values.get(it)) == "edek-" + it
        }

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting all EDEKs with complex types'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be right values'
        new String(values.get("record.int")) == "edek-record.int"
        new String(values.get("map.key1")) == "edek-map.key1"
        new String(values.get("map.key2")) == "edek-map.key2"
        new String(values.get("array.1")) == "edek-array.1"
        new String(values.get("array.2")) == "edek-array.2"
        new String(values.get("complex.record")) == "edek-complex.record"

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting all EDEKs from unencrypted data'() {
        when: 'get all encrypted values from unencrypted data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, firstRecord)

        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be 0 values'
        values.size() == 0
    }

    def 'getting all EDEKs from partially encrypted record'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be right values'
        values.size() == 3
        new String(values.get("boolean")) == "edek-boolean"
        new String(values.get("map.key1")) == "edek-map.key1"
        new String(values.get("array.1")) == "edek-array.1"

        where:
        record << [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
    }

    def 'adding unencrypted values'() {
        setup: 'initialization'
        Set<String> simpleFields = ["int", "bytes", "double", "float", "long",
                                    "string", "null", "null2", "enum", "fixed",
                                    "union", "union2"]

        when: 'add some unencrypted values to data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, encryptedRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        for (String simpleField : simpleFields) {
            dataAccessor.addUnencrypted(simpleField, AvroUtils.serialize(
                    schema.getField(simpleField).schema(),
                    record.get(simpleField)))
            dataAccessor.removeEDEK(simpleField)
        }
        dataAccessor.addUnencrypted("record.int",
                ByteUtils.serialize(((GenericRecord) record.get("record")).get("int")))
        dataAccessor.addUnencrypted("map.key2",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))))
        dataAccessor.addUnencrypted("array.2",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(1)))
        dataAccessor.addUnencrypted("complex.record",
                ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record")))
        dataAccessor.removeEDEK("record.int")
        dataAccessor.removeEDEK("map.key2")
        dataAccessor.removeEDEK("array.2")
        dataAccessor.removeEDEK("complex.record")

        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.IdRecord idRecord =
                AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right schema and records'
        idRecord.id == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedRecord.equals(idRecord.genericRecord)

        when: 'remove EDEKs from data'
        dataAccessor.deserialize(topic, bytes)
        dataAccessor.seekToNext()
        dataAccessor.removeEDEK("boolean")
        dataAccessor.removeEDEK("map.key1")
        dataAccessor.removeEDEK("array.1")
        bytes = dataAccessor.serialize()
        idRecord = AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right data'
        idRecord.id == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedNoEDEKsRecord.equals(idRecord.genericRecord)

        when: 'add rest unencrypted values to data'
        dataAccessor.deserialize(topic, bytes)
        dataAccessor.seekToNext()
        dataAccessor.addUnencrypted("boolean", AvroUtils.serialize(
                schema.getField("boolean").schema(),
                record.get("boolean")))
        dataAccessor.addUnencrypted("map.key1",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))))
        dataAccessor.addUnencrypted("array.1",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(0)))
        bytes = dataAccessor.serialize()
        idRecord = AvroTestUtils.deserializeWithoutSchema(schema, bytes)

        then: 'should be unencrypted data'
        idRecord.id == schemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        record.equals(idRecord.genericRecord)

        where:
        encryptedRecord << [encryptedFirstRecord, encryptedSecondRecord]
        record << [firstRecord, secondRecord]
        partiallyEncryptedRecord <<
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
        partiallyEncryptedNoEDEKsRecord <<
                [partiallyEncryptedFirstNoEDEKsRecord, partiallyEncryptedSecondNoEDEKsRecord]
    }

    def 'adding encrypted values'() {
        setup: 'initialization'
        Set<String> simpleFields = ["int", "bytes", "double", "float", "long",
                                    "string", "null", "null2", "enum", "fixed",
                                    "union", "union2"]
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, record)

        when: 'add some encrypted values to data'
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        dataAccessor.addEncrypted("boolean", ByteUtils.serialize(record.get("boolean")))
        dataAccessor.addEncrypted("map.key1",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))))
        dataAccessor.addEncrypted("array.1",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(0)))
        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.IdRecord idRecord =
                AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right schema and record'
        idRecord.id == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedNoEDEKsRecord.equals(idRecord.genericRecord)

        when: 'add EDEKs to data'
        dataAccessor.deserialize(topic, bytes)
        dataAccessor.seekToNext()
        dataAccessor.addEDEK("boolean", "edek-boolean".getBytes())
        dataAccessor.addEDEK("map.key1", "edek-map.key1".getBytes())
        dataAccessor.addEDEK("array.1", "edek-array.1".getBytes())

        bytes = dataAccessor.serialize()
        idRecord = AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right schema and record'
        idRecord.id == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedRecord.equals(idRecord.genericRecord)

        when: 'add rest encrypted values to data'
        dataAccessor.deserialize(topic, bytes)
        dataAccessor.seekToNext()
        for (String simpleField : simpleFields) {
            dataAccessor.addEncrypted(simpleField, AvroUtils.serialize(
                    schema.getField(simpleField).schema(), record.get(simpleField)))
            dataAccessor.addEDEK(simpleField, ("edek-" + simpleField).getBytes())
        }
        dataAccessor.addEncrypted("record.int",
                ByteUtils.serialize(((GenericRecord) record.get("record")).get("int")))
        dataAccessor.addEDEK("record.int", "edek-record.int".getBytes())
        dataAccessor.addEncrypted("map.key2",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))))
        dataAccessor.addEDEK("map.key2", "edek-map.key2".getBytes())
        dataAccessor.addEncrypted("array.2",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(1)))
        dataAccessor.addEDEK("array.2", "edek-array.2".getBytes())
        dataAccessor.addEncrypted("complex.record",
                ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record")))
        dataAccessor.addEDEK("complex.record", "edek-complex.record".getBytes())

        bytes = dataAccessor.serialize()
        idRecord = AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be encrypted data'
        idRecord.id == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        encryptedRecord.equals(idRecord.genericRecord)

        where:
        encryptedRecord << [encryptedFirstRecord, encryptedSecondRecord]
        record << [firstRecord, secondRecord]
        partiallyEncryptedRecord <<
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
        partiallyEncryptedNoEDEKsRecord <<
                [partiallyEncryptedFirstNoEDEKsRecord, partiallyEncryptedSecondNoEDEKsRecord]
    }

}
