package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.Pair
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
    GenericRecord partiallyEncryptedSecondRecord
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
                .name("encrypted").type().optional().array().items().stringType()
                .endRecord()
        encryptedSchema.addProp("initialSchemaId", schemaId)
        List<String> encrypted = ["boolean", "map.key1", "array.1", "int", "bytes",
                                  "double", "float", "long", "string", "null", "null2",
                                  "enum", "fixed", "union", "union2",  "record.int",
                                  "map.key2", "array.2", "complex.record"]
        List<String> partiallyEncrypted = ["boolean", "map.key1", "array.1"]

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
        encryptedFirstRecord.put("encrypted", encrypted)

        partiallyEncryptedFirstRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(firstRecord, partiallyEncryptedFirstRecord)
        AvroTestUtils.encryptFields(firstRecord, partiallyEncryptedFirstRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedFirstRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedFirstRecord.put("array", encryptedList)
        partiallyEncryptedFirstRecord.put("encrypted", partiallyEncrypted)

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
        encryptedSecondRecord.put("encrypted", encrypted)

        partiallyEncryptedSecondRecord = new GenericData.Record(encryptedSchema)
        AvroTestUtils.copyAllFields(secondRecord, partiallyEncryptedSecondRecord)
        AvroTestUtils.encryptFields(secondRecord, partiallyEncryptedSecondRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedSecondRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedSecondRecord.put("array", encryptedList)
        partiallyEncryptedSecondRecord.put("encrypted", partiallyEncrypted)
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
        Pair<Integer, GenericRecord> pair =
                AvroTestUtils.deserializeWithoutSchema(inputSchema, bytes)

        then: 'should be the same data'
        pair.getFirst() == inputSchemaId
        inputRecord.equals(pair.getLast())

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

    def 'getting all encrypted values with simple types'() {
        setup: 'initialization'
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "null2", "enum", "fixed",
                                   "union", "union2"]

        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEncrypted()

        then: 'should be right values'
        values.size() == 19
        fieldNames.every {
            values.get(it) == ((ByteBuffer) record.get(it)).array()
        }

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting all encrypted values with complex types'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEncrypted()

        then: 'should be right values'
        values.get("record.int") == ((ByteBuffer)
                ((GenericRecord) record.get("record")).get("int")).array()
        values.get("map.key1") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
        values.get("map.key2") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))).array()
        values.get("array.1") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(0)).array()
        values.get("array.2") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(1)).array()
        values.get("complex.record") == ((ByteBuffer)
                ((GenericRecord) record.get("complex")).get("record")).array()

        where:
        record << [encryptedFirstRecord, encryptedSecondRecord]
    }

    def 'getting all encrypted values from unencrypted data'() {
        when: 'get all encrypted values from unencrypted data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, firstRecord)

        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEncrypted()

        then: 'should be 0 values'
        values.size() == 0
    }

    def 'getting all encrypted values from partially encrypted record'() {
        when: 'get all encrypted values from data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(
                encryptedSchema, encryptedSchemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        Map<String, byte[]> values = dataAccessor.getAllEncrypted()

        then: 'should be right values'
        values.size() == 3
        values.get("boolean") == ((ByteBuffer) record.get("boolean")).array()
        values.get("map.key1") == ((ByteBuffer)
                ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
        values.get("array.1") == ((ByteBuffer)
                ((List<Object>) record.get("array")).get(0)).array()

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
        }
        dataAccessor.addUnencrypted("record.int",
                ByteUtils.serialize(((GenericRecord) record.get("record")).get("int")))
        dataAccessor.addUnencrypted("map.key2",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))))
        dataAccessor.addUnencrypted("array.2",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(1)))
        dataAccessor.addUnencrypted("complex.record",
                ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record")))

        byte[] bytes = dataAccessor.serialize()
        Pair<Integer, GenericRecord> pair =
                AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right schema and records'
        pair.getFirst() == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedRecord.equals(pair.getLast())

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
        pair = AvroTestUtils.deserializeWithoutSchema(schema, bytes)

        then: 'should be unencrypted data'
        pair.getFirst() == schemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        record.equals(pair.getLast())

        where:
        encryptedRecord << [encryptedFirstRecord, encryptedSecondRecord]
        record << [firstRecord, secondRecord]
        partiallyEncryptedRecord <<
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
    }

    def 'adding encrypted values'() {
        setup: 'initialization'
        Set<String> simpleFields = ["int", "bytes", "double", "float", "long",
                                    "string", "null", "null2", "enum", "fixed",
                                    "union", "union2"]

        when: 'add some encrypted values to data'
        byte[] data = AvroTestUtils.serializeWithoutSchema(schema, schemaId, record)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        dataAccessor.addEncrypted("boolean", ByteUtils.serialize(record.get("boolean")))
        dataAccessor.addEncrypted("map.key1",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))))
        dataAccessor.addEncrypted("array.1",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(0)))

        byte[] bytes = dataAccessor.serialize()
        Pair<Integer, GenericRecord> pair =
                AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be right schema and record'
        pair.getFirst() == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        partiallyEncryptedRecord.equals(pair.getLast())

        when: 'add rest encrypted values to data'
        dataAccessor.deserialize(topic, bytes)
        dataAccessor.seekToNext()
        for (String simpleField : simpleFields) {
            dataAccessor.addEncrypted(simpleField, AvroUtils.serialize(
                    schema.getField(simpleField).schema(), record.get(simpleField)))
        }
        dataAccessor.addEncrypted("record.int",
                ByteUtils.serialize(((GenericRecord) record.get("record")).get("int")))
        dataAccessor.addEncrypted("map.key2",
                ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))))
        dataAccessor.addEncrypted("array.2",
                ByteUtils.serialize(((List<Object>) record.get("array")).get(1)))
        dataAccessor.addEncrypted("complex.record",
                ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record")))

        bytes = dataAccessor.serialize()
        pair = AvroTestUtils.deserializeWithoutSchema(encryptedSchema, bytes)

        then: 'should be encrypted data'
        pair.getFirst() == encryptedSchemaId
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        encryptedRecord.equals(pair.getLast())

        where:
        encryptedRecord << [encryptedFirstRecord, encryptedSecondRecord]
        record << [firstRecord, secondRecord]
        partiallyEncryptedRecord <<
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
    }

}
