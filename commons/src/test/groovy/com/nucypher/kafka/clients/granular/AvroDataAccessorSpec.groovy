package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.AvroUtils
import com.nucypher.kafka.utils.ByteUtils
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer

/**
 * Test for {@link AvroDataAccessor}
 */
class AvroDataAccessorSpec extends Specification {

    @Shared
    Schema schema = AvroTestUtils.getSchema()
    @Shared
    Schema partiallyEncryptedSchema
    @Shared
    Schema partiallyEncryptedNoEDEKsSchema
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
    GenericRecord partiallyEncryptedFirstNoEDEKsRecord
    @Shared
    GenericRecord partiallyEncryptedSecondNoEDEKsRecord
    @Shared
    String topic = "topic"

    def setupSpec() {
        Schema innerRecordSchema = AvroTestUtils.getInnerRecordSchema()
        Schema complexInnerRecordSchema = AvroTestUtils.getComplexInnerRecordSchema()
        Schema enumSchema = AvroTestUtils.getEnumSchema()
        Schema fixedSchema = AvroTestUtils.getFixedSchema()

        Schema encryptedInnerRecordSchema = SchemaBuilder.record("record1").fields()
                .name("int").type().bytesType().noDefault().endRecord()
        Schema encryptedComplexInnerRecordSchema = SchemaBuilder.record("record2").fields()
                .name("record").type().bytesType().noDefault().endRecord()

        encryptedSchema = SchemaBuilder.record("record").namespace("namespace")
                .fields()
                .name("int").aliases("int_alias").doc("doc").type(
                SchemaBuilder.builder().bytesType()).withDefault(1)
                .name("unencrypted").type().intType().intDefault(1)
                .name("boolean").type().bytesType().noDefault()
                .name("bytes").type().bytesType().noDefault()
                .name("double").type().bytesType().noDefault()
                .name("float").type().bytesType().noDefault()
                .name("long").type().bytesType().noDefault()
                .name("string").type().bytesType().noDefault()
                .name("null").type(SchemaBuilder.builder().bytesType()).withDefault(null)
                .name("null2").type(SchemaBuilder.builder().bytesType()).withDefault(null)
                .name("enum").type().bytesType().noDefault()
                .name("fixed").type().bytesType().noDefault()
                .name("record").type(encryptedInnerRecordSchema).noDefault()
                .name("map").type().map().values().unionOf()
                .intType().and().bytesType().endUnion().noDefault()
                .name("array").type().array().items().bytesType().noDefault()
                .name("union").type().bytesType().noDefault()
                .name("union2").type().bytesType().noDefault()
                .name("complex").type(encryptedComplexInnerRecordSchema).noDefault()
                .endRecord()
        Map<String, String> encrypted = new HashMap<>()
        encrypted.put("int", SchemaBuilder.builder().intType().toString())
        encrypted.put("boolean", SchemaBuilder.builder().booleanType().toString())
        encrypted.put("bytes", SchemaBuilder.builder().bytesType().toString())
        encrypted.put("double", SchemaBuilder.builder().doubleType().toString())
        encrypted.put("float", SchemaBuilder.builder().floatType().toString())
        encrypted.put("long", SchemaBuilder.builder().longType().toString())
        encrypted.put("string", SchemaBuilder.builder().stringType().toString())
        encrypted.put("null", SchemaBuilder.builder().nullType().toString())
        encrypted.put("null2", SchemaBuilder.unionOf().nullType().and()
                .intType().endUnion().toString())
        encrypted.put("enum", enumSchema.toString())
        encrypted.put("fixed", fixedSchema.toString())
        encrypted.put("record.int", SchemaBuilder.builder().intType().toString())
        encrypted.put("map.key1", SchemaBuilder.builder().intType().toString())
        encrypted.put("map.key2", SchemaBuilder.builder().intType().toString())
        encrypted.put("array.1", SchemaBuilder.builder().bytesType().toString())
        encrypted.put("array.2", SchemaBuilder.builder().bytesType().toString())
        encrypted.put("union", SchemaBuilder.unionOf()
                .intType().and().stringType().endUnion().toString())
        encrypted.put("union2", SchemaBuilder.unionOf()
                .map().values().intType().and().intType().endUnion().toString())
        encrypted.put("complex.record", innerRecordSchema.toString())
        encryptedSchema.addProp("encrypted", encrypted)
        Map<String, String> edeks = new HashMap<>()
        AvroTestUtils.addEDEKs(edeks, "int", "boolean", "bytes", "double",
                "float", "long", "string", "null", "null2", "enum", "fixed",
                "record.int", "map.key1", "map.key2", "array.1", "array.2",
                "union", "union2", "complex.record")
        encryptedSchema.addProp("edeks", edeks)

        partiallyEncryptedNoEDEKsSchema = SchemaBuilder
                .record("record").namespace("namespace").fields()
                .name("int").aliases("int_alias").doc("doc").type().intType().intDefault(1)
                .name("unencrypted").type().intType().intDefault(1)
                .name("boolean").type().bytesType().noDefault()
                .name("bytes").type().bytesType().noDefault()
                .name("double").type().doubleType().noDefault()
                .name("float").type().floatType().noDefault()
                .name("long").type().longType().noDefault()
                .name("string").type().stringType().noDefault()
                .name("null").type().nullType().nullDefault()
                .name("null2").type().optional().intType()
                .name("enum").type(enumSchema).noDefault()
                .name("fixed").type(fixedSchema).noDefault()
                .name("record").type(innerRecordSchema).noDefault()
                .name("map").type().map().values().unionOf()
                .intType().and().bytesType().endUnion().noDefault()
                .name("array").type().array().items().bytesType().noDefault()
                .name("union").type().unionOf()
                .intType().and().stringType().endUnion().noDefault()
                .name("union2").type().unionOf()
                .map().values().intType().and().intType().endUnion().noDefault()
                .name("complex").type(complexInnerRecordSchema).noDefault()
                .endRecord()
        encrypted = new HashMap<>()
        encrypted.put("boolean", SchemaBuilder.builder().booleanType().toString())
        encrypted.put("map.key1", SchemaBuilder.builder().intType().toString())
        encrypted.put("array.1", SchemaBuilder.builder().bytesType().toString())
        partiallyEncryptedNoEDEKsSchema.addProp("encrypted", encrypted)
        partiallyEncryptedSchema = new Schema.Parser().parse(
                partiallyEncryptedNoEDEKsSchema.toString())
        edeks = new HashMap<>()
        AvroTestUtils.addEDEKs(edeks, "boolean", "map.key1", "array.1")
        partiallyEncryptedSchema.addProp("edeks", edeks)

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

        partiallyEncryptedFirstRecord = new GenericData.Record(partiallyEncryptedSchema)
        AvroTestUtils.copyAllFields(firstRecord, partiallyEncryptedFirstRecord)
        AvroTestUtils.encryptFields(firstRecord, partiallyEncryptedFirstRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedFirstRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedFirstRecord.put("array", encryptedList)

        partiallyEncryptedFirstNoEDEKsRecord = new GenericData.Record(
                partiallyEncryptedNoEDEKsSchema)
        AvroTestUtils.copyAllFields(partiallyEncryptedFirstRecord,
                partiallyEncryptedFirstNoEDEKsRecord)

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

        partiallyEncryptedSecondRecord = new GenericData.Record(partiallyEncryptedSchema)
        AvroTestUtils.copyAllFields(secondRecord, partiallyEncryptedSecondRecord)
        AvroTestUtils.encryptFields(secondRecord, partiallyEncryptedSecondRecord, "boolean")
        encryptedMap = new HashMap<>(map)
        AvroTestUtils.encryptMap(encryptedMap, "key1")
        partiallyEncryptedSecondRecord.put("map", encryptedMap)
        encryptedList = new ArrayList<>(list)
        AvroTestUtils.encryptList(encryptedList, 0)
        partiallyEncryptedSecondRecord.put("array", encryptedList)

        partiallyEncryptedSecondNoEDEKsRecord = new GenericData.Record(
                partiallyEncryptedNoEDEKsSchema)
        AvroTestUtils.copyAllFields(partiallyEncryptedSecondRecord,
                partiallyEncryptedSecondNoEDEKsRecord)
    }

    def 'getting next element'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        byte[] data = AvroTestUtils.serialize(schema, firstRecord)

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
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        !dataAccessor.hasNext()

        when: 'deserialize two record'
        data = AvroTestUtils.serialize(schema, firstRecord, secondRecord)
        dataAccessor.deserialize(topic, data)

        then: 'accessor has two elements'
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        !dataAccessor.hasNext()

        when: 'seek to the next element'
        dataAccessor.seekToNext()

        then: 'thrown NoSuchElementException exception'
        thrown(NoSuchElementException)

        when: 'reset accessor'
        dataAccessor.reset()

        then: 'accessor has two elements'
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        dataAccessor.hasNext()
        dataAccessor.seekToNext()
        !dataAccessor.hasNext()
    }

    def 'serialization and deserialization'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        Schema parsedSchema
        List<GenericRecord> records

        when: 'deserialize and serialize Avro bytes'
        byte[] data = AvroTestUtils.serialize(inputSchema, *inputRecords)
        dataAccessor.deserialize(topic, data)
        while (dataAccessor.hasNext()) {
            dataAccessor.seekToNext()
        }
        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.SchemaRecords schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        records = schemaRecords.genericRecords

        then: 'should be the same data'
        parsedSchema == inputSchema
        records.size() == inputRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < inputRecords.size(); i++) {
            assert inputRecords[i].equals(records[i])
        }

        where:
        inputSchema << [
                schema,
                schema,
                encryptedSchema,
                encryptedSchema,
                partiallyEncryptedSchema,
                partiallyEncryptedSchema
        ]
        inputRecords << [
                [firstRecord],
                [firstRecord, secondRecord],
                [encryptedFirstRecord],
                [encryptedFirstRecord, encryptedSecondRecord],
                [partiallyEncryptedFirstRecord],
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
        ]
    }

    def 'getting all fields'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get all fields from record'
        byte[] data = AvroTestUtils.serialize(schema, *inputRecords)
        dataAccessor.deserialize(topic, data)

        then: 'should be all fields'
        dataAccessor.getAllFields() ==
                ["int", "boolean", "bytes", "double", "float",
                 "long", "string", "null", "null2", "enum", "fixed",
                 "record.int", "map.key1", "map.key2", "array.1",
                 "array.2", "union", "union2", "complex.record.int",
                 "unencrypted"].toSet()

        where:
        inputRecords << [[firstRecord],
                         [firstRecord, secondRecord],
                         [secondRecord, firstRecord]]
    }

    def 'getting value by field name with simple types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        byte[] data = AvroTestUtils.serialize(schema, *records)
        dataAccessor.deserialize(topic, data)
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "enum", "fixed",
                                   "null2", "union", "union2"]

        when: 'get value of field from data'

        then: 'should be right value'
        records.every {
            GenericRecord record = it
            dataAccessor.seekToNext()
            fieldNames.every {
                byte[] unencryptedField = dataAccessor.getUnencrypted(it)
                unencryptedField == AvroUtils.serialize(
                        schema.getField(it).schema(), record.get(it))
            }
        }

        where:
        records << [[firstRecord], [secondRecord], [firstRecord, secondRecord]]
    }

    def 'getting value by field name with complex types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        byte[] data = AvroTestUtils.serialize(schema, *records)
        dataAccessor.deserialize(topic, data)

        when: 'get value of field from data'

        then: 'should be right value'
        records.forEach {
            GenericRecord record = it
            dataAccessor.seekToNext()
            assert dataAccessor.getUnencrypted("record.int") ==
                    ByteUtils.serialize(((GenericRecord) record.get("record")).get("int"))
            assert dataAccessor.getUnencrypted("map.key1") ==
                    ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1")))
            assert dataAccessor.getUnencrypted("map.key2") ==
                    ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2")))
            assert dataAccessor.getUnencrypted("array.1") ==
                    ByteUtils.serialize(((List<Object>) record.get("array")).get(0))
            assert dataAccessor.getUnencrypted("array.2") ==
                    ByteUtils.serialize(((List<Object>) record.get("array")).get(1))
            assert dataAccessor.getUnencrypted("complex.record") ==
                    ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record"))
        }

        where:
        records << [[firstRecord], [secondRecord], [firstRecord, secondRecord]]
    }

    def 'getting value by nonexistent field'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        byte[] data = AvroTestUtils.serialize(schema, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        when: 'get value of nonexistent field'
        dataAccessor.getUnencrypted(fieldName)

        then: 'thrown not-found exception'
        thrown(CommonException)

        where:
        fieldName << ["test", "record.int2", "array.3", "map.key3", "union3.record"]
    }

    def 'getting all EDEKs with simple types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "null2", "enum", "fixed",
                                   "union", "union2"]

        when: 'get all EDEKs from data'
        byte[] data = AvroTestUtils.serialize(encryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.forEach {
            dataAccessor.seekToNext()
            Map<String, byte[]> values = dataAccessor.getAllEDEKs()
            assert values.size() == 19
            fieldNames.forEach {
                assert new String(values.get(it)) == "edek-" + it
            }
        }

        where:
        records << [[encryptedFirstRecord],
                    [encryptedSecondRecord],
                    [encryptedSecondRecord, encryptedFirstRecord],
                    [encryptedFirstRecord, encryptedSecondRecord]]
    }

    def 'getting all EDEKs with complex types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get all EDEKs from data'
        byte[] data = AvroTestUtils.serialize(encryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.forEach {
            dataAccessor.seekToNext()
            Map<String, byte[]> values = dataAccessor.getAllEDEKs()
            assert new String(values.get("record.int")) == "edek-record.int"
            assert new String(values.get("map.key1")) == "edek-map.key1"
            assert new String(values.get("map.key2")) == "edek-map.key2"
            assert new String(values.get("array.1")) == "edek-array.1"
            assert new String(values.get("array.2")) == "edek-array.2"
            assert new String(values.get("complex.record")) == "edek-complex.record"
        }

        where:
        records << [[encryptedFirstRecord],
                    [encryptedSecondRecord],
                    [encryptedFirstRecord, encryptedSecondRecord]]
    }

    def 'getting all EDEKs from unencrypted data'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get all EDEKs from unencrypted data'
        byte[] data = AvroTestUtils.serialize(schema, firstRecord)
        dataAccessor.deserialize(topic, data)
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be 0 values'
        values.size() == 0
    }

    def 'getting all EDEKs from partially encrypted record'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get all EDEKs from data'
        byte[] data = AvroTestUtils.serialize(partiallyEncryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.forEach {
            dataAccessor.seekToNext()
            Map<String, byte[]> values = dataAccessor.getAllEDEKs()
            values.size() == 3
            assert new String(values.get("boolean")) == "edek-boolean"
            assert new String(values.get("map.key1")) == "edek-map.key1"
            assert new String(values.get("array.1")) == "edek-array.1"
        }

        where:
        records << [[partiallyEncryptedFirstRecord],
                    [partiallyEncryptedSecondRecord],
                    [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]]
    }

    def 'getting encrypted values with simple types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        List<String> fieldNames = ["int", "boolean", "bytes", "double", "float",
                                   "long", "string", "null", "null2", "enum", "fixed",
                                   "union", "union2"]

        when: 'get encrypted values from data'
        byte[] data = AvroTestUtils.serialize(encryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.every {
            GenericRecord record = it
            dataAccessor.seekToNext()
            fieldNames.every {
                byte[] encryptedField = dataAccessor.getEncrypted(it)
                encryptedField == ((ByteBuffer) record.get(it)).array()
            }
        }

        where:
        records << [[encryptedFirstRecord],
                    [encryptedSecondRecord],
                    [encryptedSecondRecord, encryptedFirstRecord],
                    [encryptedFirstRecord, encryptedSecondRecord]]
    }

    def 'getting encrypted values with complex types'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get encrypted values from data'
        byte[] data = AvroTestUtils.serialize(encryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.forEach {
            GenericRecord record = it
            dataAccessor.seekToNext()
            assert dataAccessor.getEncrypted("record.int") == ((ByteBuffer)
                    ((GenericRecord) record.get("record")).get("int")).array()
            assert dataAccessor.getEncrypted("map.key1") == ((ByteBuffer)
                    ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
            assert dataAccessor.getEncrypted("map.key2") == ((ByteBuffer)
                    ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))).array()
            assert dataAccessor.getEncrypted("array.1") == ((ByteBuffer)
                    ((List<Object>) record.get("array")).get(0)).array()
            assert dataAccessor.getEncrypted("array.2") == ((ByteBuffer)
                    ((List<Object>) record.get("array")).get(1)).array()
            assert dataAccessor.getEncrypted("complex.record") == ((ByteBuffer)
                    ((GenericRecord) record.get("complex")).get("record")).array()
        }

        where:
        records << [[encryptedFirstRecord],
                    [encryptedSecondRecord],
                    [encryptedFirstRecord, encryptedSecondRecord]]
    }

    def 'getting encrypted values by nonexistent field'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        byte[] data = AvroTestUtils.serialize(schema, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()

        when: 'get value of nonexistent field'
        dataAccessor.getEncrypted(fieldName)

        then: 'thrown not-found exception'
        thrown(CommonException)

        where:
        fieldName << ["test", "record.int2", "array.3", "map.key3", "union3.record"]
    }

    def 'getting encrypted values from partially encrypted record'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'get encrypted values from data'
        byte[] data = AvroTestUtils.serialize(partiallyEncryptedSchema, *records)
        dataAccessor.deserialize(topic, data)

        then: 'should be right values'
        records.forEach {
            GenericRecord record = it
            dataAccessor.seekToNext()
            assert dataAccessor.getEncrypted("boolean") == ((ByteBuffer) record.get("boolean")).array()
            assert dataAccessor.getEncrypted("map.key1") == ((ByteBuffer)
                    ((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))).array()
            assert dataAccessor.getEncrypted("array.1") == ((ByteBuffer)
                    ((List<Object>) record.get("array")).get(0)).array()
        }

        where:
        records << [[partiallyEncryptedFirstRecord],
                    [partiallyEncryptedSecondRecord],
                    [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]]
    }

    def 'adding unencrypted values'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        Set<String> simpleFields = ["int", "bytes", "double", "float", "long",
                                    "string", "null", "null2", "enum", "fixed",
                                    "union", "union2"]
        Schema parsedSchema
        List<GenericRecord> parsedRecords

        when: 'add some unencrypted values to data'
        byte[] data = AvroTestUtils.serialize(encryptedSchema, *encryptedRecords)
        dataAccessor.deserialize(topic, data)
        for (GenericRecord record : records) {
            dataAccessor.seekToNext()
            for (String simpleField : simpleFields) {
                dataAccessor.addUnencrypted(simpleField, AvroUtils.serialize(
                        schema.getField(simpleField).schema(),
                        record.get(simpleField)))
                dataAccessor.removeEDEK(simpleField)
            }
            dataAccessor.addUnencrypted("record.int",
                    ByteUtils.serialize(((GenericRecord) record.get("record")).get("int")))
            dataAccessor.removeEDEK("record.int")
            dataAccessor.addUnencrypted("map.key2",
                    ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key2"))))
            dataAccessor.removeEDEK("map.key2")
            dataAccessor.addUnencrypted("array.2",
                    ByteUtils.serialize(((List<Object>) record.get("array")).get(1)))
            dataAccessor.removeEDEK("array.2")
            dataAccessor.addUnencrypted("complex.record",
                    ByteUtils.serialize(((GenericRecord) record.get("complex")).get("record")))
            dataAccessor.removeEDEK("complex.record")
        }
        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.SchemaRecords schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be right schema and records'
        parsedSchema == partiallyEncryptedSchema
        parsedRecords.size() == partiallyEncryptedRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < partiallyEncryptedRecords.size(); i++) {
            assert partiallyEncryptedRecords[i].equals(parsedRecords[i])
        }

        when: 'remove all EDEKs'
        dataAccessor.deserialize(topic, bytes)
        for (GenericRecord record : records) {
            dataAccessor.seekToNext()
            dataAccessor.removeEDEK("boolean")
            dataAccessor.removeEDEK("map.key1")
            dataAccessor.removeEDEK("array.1")
        }
        bytes = dataAccessor.serialize()
        schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be right data'
        parsedSchema == partiallyEncryptedNoEDEKsSchema
        parsedRecords.size() == partiallyEncryptedNoEDEKsRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < partiallyEncryptedNoEDEKsRecords.size(); i++) {
            assert partiallyEncryptedNoEDEKsRecords[i].equals(parsedRecords[i])
        }

        when: 'add rest unencrypted values to data'
        dataAccessor.deserialize(topic, bytes)
        for (GenericRecord record : records) {
            dataAccessor.seekToNext()
            dataAccessor.addUnencrypted("boolean", AvroUtils.serialize(
                    schema.getField("boolean").schema(),
                    record.get("boolean")))
            dataAccessor.addUnencrypted("map.key1",
                    ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))))
            dataAccessor.addUnencrypted("array.1",
                    ByteUtils.serialize(((List<Object>) record.get("array")).get(0)))
        }

        bytes = dataAccessor.serialize()
        schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be unencrypted data'
        parsedSchema == schema
        parsedRecords.size() == records.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < records.size(); i++) {
            assert records[i].equals(parsedRecords[i])
        }

        where:
        encryptedRecords << [
                [encryptedFirstRecord],
                [encryptedSecondRecord],
                [encryptedFirstRecord, encryptedSecondRecord]
        ]
        records << [
                [firstRecord],
                [secondRecord],
                [firstRecord, secondRecord]
        ]
        partiallyEncryptedRecords << [
                [partiallyEncryptedFirstRecord],
                [partiallyEncryptedSecondRecord],
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]
        ]
        partiallyEncryptedNoEDEKsRecords << [
                [partiallyEncryptedFirstNoEDEKsRecord],
                [partiallyEncryptedSecondNoEDEKsRecord],
                [partiallyEncryptedFirstNoEDEKsRecord, partiallyEncryptedSecondNoEDEKsRecord]
        ]
    }

    def 'adding encrypted values'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()
        Set<String> simpleFields = ["int", "bytes", "double", "float", "long",
                                    "string", "null", "null2", "enum", "fixed",
                                    "union", "union2"]
        Schema parsedSchema
        List<GenericRecord> parsedRecords
        byte[] data = AvroTestUtils.serialize(schema, *records)

        when: 'add some encrypted values to data'
        dataAccessor.deserialize(topic, data)
        for (GenericRecord record : records) {
            dataAccessor.seekToNext()
            dataAccessor.addEncrypted("boolean", ByteUtils.serialize(record.get("boolean")))
            dataAccessor.addEncrypted("map.key1",
                    ByteUtils.serialize(((Map<Utf8, Object>) record.get("map")).get(new Utf8("key1"))))
            dataAccessor.addEncrypted("array.1",
                    ByteUtils.serialize(((List<Object>) record.get("array")).get(0)))
        }

        byte[] bytes = dataAccessor.serialize()
        AvroTestUtils.SchemaRecords schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be right schema and record'
        parsedSchema == partiallyEncryptedNoEDEKsSchema
        parsedRecords.size() == partiallyEncryptedNoEDEKsRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < partiallyEncryptedNoEDEKsRecords.size(); i++) {
            assert partiallyEncryptedNoEDEKsRecords[i].equals(parsedRecords[i])
        }

        when: 'add EDEKs to data'
        dataAccessor.deserialize(topic, bytes)
        for (GenericRecord record : records) {
            dataAccessor.seekToNext()
            dataAccessor.addEDEK("boolean", "edek-boolean".getBytes())
            dataAccessor.addEDEK("map.key1", "edek-map.key1".getBytes())
            dataAccessor.addEDEK("array.1", "edek-array.1".getBytes())
        }

        bytes = dataAccessor.serialize()
        schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be right schema and record'
        parsedSchema == partiallyEncryptedSchema
        parsedRecords.size() == partiallyEncryptedRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < partiallyEncryptedRecords.size(); i++) {
            assert partiallyEncryptedRecords[i].equals(parsedRecords[i])
        }

        when: 'add rest encrypted values to data'
        dataAccessor.deserialize(topic, bytes)

        for (GenericRecord record : records) {
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
        }

        bytes = dataAccessor.serialize()
        schemaRecords = AvroTestUtils.deserialize(bytes)
        parsedSchema = schemaRecords.schema
        parsedRecords = schemaRecords.genericRecords

        then: 'should be encrypted data'
        parsedSchema == encryptedSchema
        parsedRecords.size() == encryptedRecords.size()
        //need equals method otherwise AvroRuntimeException: Can't compare maps!
        for (int i = 0; i < encryptedRecords.size(); i++) {
            assert encryptedRecords[i].equals(parsedRecords[i])
        }

        where:
        encryptedRecords << [
                [encryptedFirstRecord],
                [encryptedSecondRecord],
                [encryptedFirstRecord, encryptedSecondRecord]]
        records << [
                [firstRecord],
                [secondRecord],
                [firstRecord, secondRecord]]
        partiallyEncryptedRecords << [
                [partiallyEncryptedFirstRecord],
                [partiallyEncryptedSecondRecord],
                [partiallyEncryptedFirstRecord, partiallyEncryptedSecondRecord]]
        partiallyEncryptedNoEDEKsRecords << [
                [partiallyEncryptedFirstNoEDEKsRecord],
                [partiallyEncryptedSecondNoEDEKsRecord],
                [partiallyEncryptedFirstNoEDEKsRecord, partiallyEncryptedSecondNoEDEKsRecord]]
    }

    def 'using schemas cache'() {
        setup: 'initialization'
        AvroDataAccessor dataAccessor = new AvroDataAccessor()

        when: 'add some encrypted values to data'
        byte[] data = AvroTestUtils.serialize(schema, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        dataAccessor.addEncrypted("boolean", ByteUtils.serialize(firstRecord.get("boolean")))
        dataAccessor.addEDEK("boolean", "edek-boolean".getBytes())
        dataAccessor.addEncrypted("map.key1", ByteUtils.serialize(
                ((Map<Utf8, Object>) firstRecord.get("map")).get(new Utf8("key1"))))
        dataAccessor.addEDEK("map.key1", "edek-map.key1".getBytes())
        dataAccessor.addEncrypted("array.1",
                ByteUtils.serialize(((List<Object>) firstRecord.get("array")).get(0)))
        dataAccessor.addEDEK("array.1", "edek-array.1".getBytes())

        byte[] bytes = dataAccessor.serialize()
        Schema firstSchema = AvroTestUtils.deserialize(bytes).schema

        data = AvroTestUtils.serialize(schema, secondRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        dataAccessor.addEncrypted("boolean", ByteUtils.serialize(secondRecord.get("boolean")))
        dataAccessor.addEDEK("boolean", "edek-boolean".getBytes())
        dataAccessor.addEncrypted("map.key1", ByteUtils.serialize(
                ((Map<Utf8, Object>) secondRecord.get("map")).get(new Utf8("key1"))))
        dataAccessor.addEDEK("map.key1", "edek-map.key1".getBytes())
        dataAccessor.addEncrypted("array.1",
                ByteUtils.serialize(((List<Object>) secondRecord.get("array")).get(0)))
        dataAccessor.addEDEK("array.1", "edek-array.1".getBytes())

        bytes = dataAccessor.serialize()
        Schema secondSchema = AvroTestUtils.deserialize(bytes).schema

        then: 'should be identical schemas'
        firstSchema == secondSchema

        when: 'add new EDEK to data'
        data = AvroTestUtils.serialize(schema, firstRecord)
        dataAccessor.deserialize(topic, data)
        dataAccessor.seekToNext()
        dataAccessor.addEncrypted("boolean", ByteUtils.serialize(secondRecord.get("boolean")))
        dataAccessor.addEDEK("boolean", "edek-boolean2".getBytes())
        dataAccessor.addEncrypted("map.key1", ByteUtils.serialize(
                ((Map<Utf8, Object>) secondRecord.get("map")).get(new Utf8("key1"))))
        dataAccessor.addEDEK("map.key1", "edek-map.key1".getBytes())
        dataAccessor.addEncrypted("array.1",
                ByteUtils.serialize(((List<Object>) secondRecord.get("array")).get(0)))
        dataAccessor.addEDEK("array.1", "edek-array.1".getBytes())
        bytes = dataAccessor.serialize()
        Schema thirdSchema = AvroTestUtils.deserialize(bytes).schema

        then: 'should be different schemas'
        thirdSchema != secondSchema
    }

}
