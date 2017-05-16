package com.nucypher.kafka.clients.granular;

import com.nucypher.kafka.Pair;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro utils for tests
 */
public class AvroTestUtils {
    private static final byte MAGIC_BYTE = 0x0;
    private static final int ID_SIZE = 4;

    private AvroTestUtils() {
    }

//    private static final Random RANDOM = new Random();

    private static Schema schema;
    private static Schema innerRecordSchema;
    private static Schema complexInnerRecordSchema;
    private static Schema enumSchema;
    private static Schema fixedSchema;

    private static GenericRecord firstRecord;
    private static GenericRecord secondRecord;


    static {
        innerRecordSchema = SchemaBuilder.record("record1").fields()
                .name("int").type().intType().noDefault().endRecord();

        complexInnerRecordSchema = SchemaBuilder.record("record2").fields()
                .name("record").type(innerRecordSchema).noDefault().endRecord();

        enumSchema = SchemaBuilder.enumeration("enum").symbols("e1", "e2");
        fixedSchema = SchemaBuilder.fixed("fixed").size(4);

        schema = SchemaBuilder.record("record").namespace("namespace")
                .fields()
                .name("int").aliases("int_alias").doc("doc").type().intType().intDefault(1)
                .name("unencrypted").type().intType().intDefault(1)
                .name("boolean").type().booleanType().noDefault()
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
                .name("map").type().map().values().intType().noDefault()
                .name("array").type().array().items().bytesType().noDefault()
                .name("union").type().unionOf()
                .intType().and().stringType().endUnion().noDefault()
                .name("union2").type().unionOf()
                .map().values().intType().and().intType().endUnion().noDefault()
                .name("complex").type(complexInnerRecordSchema).noDefault()
                .endRecord();

        firstRecord = new GenericData.Record(schema);
        firstRecord.put("int", 1);
        firstRecord.put("unencrypted", 3);
        firstRecord.put("boolean", true);
        firstRecord.put("bytes", ByteBuffer.wrap("byt".getBytes()));
        firstRecord.put("double", 10d);
        firstRecord.put("float", 11f);
        firstRecord.put("long", 12L);
        firstRecord.put("string", new Utf8("str"));
        firstRecord.put("null", null);
        firstRecord.put("null2", null);
        firstRecord.put("enum", new GenericData.EnumSymbol(enumSchema, "e1"));
        firstRecord.put("fixed", new GenericData.Fixed(fixedSchema, "fix1".getBytes()));
        GenericRecord innerRecord = new GenericData.Record(innerRecordSchema);
        innerRecord.put("int", 4);
        firstRecord.put("record", innerRecord);
        Map<Utf8, Object> map1 = new HashMap<>();
        map1.put(new Utf8("key1"), 5);
        map1.put(new Utf8("key2"), 6);
        firstRecord.put("map", map1);
        List<Object> list = new ArrayList<>();
        list.add(ByteBuffer.wrap("1".getBytes()));
        list.add(ByteBuffer.wrap("2".getBytes()));
        firstRecord.put("array", list);
        firstRecord.put("union", 7);
        Map<Utf8, Object> map2 = new HashMap<>();
        map2.put(new Utf8("int"), 53);
        firstRecord.put("union2", map2);
        GenericRecord complexInnerRecord = new GenericData.Record(complexInnerRecordSchema);
        complexInnerRecord.put("record", innerRecord);
        firstRecord.put("complex", complexInnerRecord);

        secondRecord = new GenericData.Record(schema);
        secondRecord.put("int", 2);
        secondRecord.put("unencrypted", 4);
        secondRecord.put("boolean", false);
        secondRecord.put("bytes", ByteBuffer.wrap("byt1".getBytes()));
        secondRecord.put("double", 110d);
        secondRecord.put("float", 111f);
        secondRecord.put("long", 112L);
        secondRecord.put("string", new Utf8("str1"));
        secondRecord.put("null", null);
        secondRecord.put("null2", 33);
        secondRecord.put("enum", new GenericData.EnumSymbol(enumSchema, "e2"));
        secondRecord.put("fixed", new GenericData.Fixed(fixedSchema, "fix2".getBytes()));
        innerRecord = new GenericData.Record(innerRecordSchema);
        innerRecord.put("int", 14);
        secondRecord.put("record", innerRecord);
        map1 = new HashMap<>();
        map1.put(new Utf8("key1"), 15);
        map1.put(new Utf8("key2"), 16);
        secondRecord.put("map", map1);
        list = new ArrayList<>();
        list.add(ByteBuffer.wrap("11".getBytes()));
        list.add(ByteBuffer.wrap("12".getBytes()));
        secondRecord.put("array", list);
        secondRecord.put("union", new Utf8("str2"));
        secondRecord.put("union2", 10);
        complexInnerRecord = new GenericData.Record(complexInnerRecordSchema);
        complexInnerRecord.put("record", innerRecord);
        secondRecord.put("complex", complexInnerRecord);
    }

    /**
     * @return initial schema
     */
    public static Schema getSchema() {
        return schema;
    }

    /**
     * @return inner record schema
     */
    public static Schema getInnerRecordSchema() {
        return innerRecordSchema;
    }

    /**
     * @return comlex inner record schema
     */
    public static Schema getComplexInnerRecordSchema() {
        return complexInnerRecordSchema;
    }

    /**
     * @return enum schema
     */
    public static Schema getEnumSchema() {
        return enumSchema;
    }

    /**
     * @return fixed schema
     */
    public static Schema getFixedSchema() {
        return fixedSchema;
    }

    /**
     * @return unencrypted first record
     */
    public static GenericRecord getFirstRecord() {
        return firstRecord;
    }

    /**
     * @return unencrypted second record
     */
    public static GenericRecord getSecondRecord() {
        return secondRecord;
    }

    //    /**
//     * @return unencrypted record
//     */
//    public static GenericRecord getRecord() {
//        GenericRecord record = new GenericData.Record(schema);
//        record.put("int", RANDOM.nextInt());
//        record.put("unencrypted", RANDOM.nextInt());
//        record.put("boolean", RANDOM.nextBoolean());
//        record.put("bytes", ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
//        record.put("double", RANDOM.nextDouble());
//        record.put("float", RANDOM.nextFloat());
//        record.put("long", RANDOM.nextLong());
//        record.put("string", new Utf8(UUID.randomUUID().toString()));
//        record.put("null", null);
//        if (RANDOM.nextBoolean()) {
//            record.put("null2", null);
//        } else {
//            record.put("null2", RANDOM.nextInt());
//        }
//        if (RANDOM.nextBoolean()) {
//            record.put("enum", new GenericData.EnumSymbol(enumSchema, "e1"));
//        } else {
//            record.put("enum", new GenericData.EnumSymbol(enumSchema, "e2"));
//        }
//        record.put("fixed", new GenericData.Fixed(
//                fixedSchema, UUID.randomUUID().toString().getBytes()));
//        GenericRecord innerRecord = new GenericData.Record(innerRecordSchema);
//        innerRecord.put("int", 4);
//        record.put("record", innerRecord);
//        Map<Utf8, Object> map1 = new HashMap<>();
//        map1.put(new Utf8("key1"), 5);
//        map1.put(new Utf8("key2"), 6);
//        record.put("map", map1);
//        List<Object> list = new ArrayList<>();
//        list.add(ByteBuffer.wrap("1".getBytes()));
//        list.add(ByteBuffer.wrap("2".getBytes()));
//        record.put("array", list);
//        record.put("union", 7);
//        Map<Utf8, Integer> map2 = new HashMap<>();
//        map2.put(new Utf8("int"), 53);
//        record.put("union2", map2);
//        record.put("union3", map2);
//        GenericRecord complexInnerRecord = new GenericData.Record(complexInnerRecordSchema);
//        complexInnerRecord.put("record", innerRecord);
//        record.put("complex", complexInnerRecord);
//        return record;
//    }

    /**
     * Copy all fields from input record and put it to the new record
     *
     * @param recordFrom input record
     * @param recordTo   new record
     */
    public static void copyAllFields(GenericRecord recordFrom, GenericRecord recordTo) {
        for (Schema.Field field : recordFrom.getSchema().getFields()) {
            String fieldName = field.name();
            Object value = recordFrom.get(fieldName);
            recordTo.put(fieldName, value);
        }
    }

    /**
     * Encrypt fields from input record and put it to the new record
     *
     * @param recordFrom input record
     * @param recordTo   new record
     * @param fields     fields to encrypt
     */
    public static void encryptFields(GenericRecord recordFrom, GenericRecord recordTo, String... fields) {
        for (String field : fields) {
            Object value = recordFrom.get(field);
            Schema schema = recordFrom.getSchema().getField(field).schema();
            byte[] bytes = AvroUtils.serialize(schema, value);
            recordTo.put(field, ByteBuffer.wrap(bytes));
        }
    }

    /**
     * Encrypt fields from map
     *
     * @param map    map
     * @param fields fields to encrypt
     */
    public static void encryptMap(Map<Utf8, Object> map, String... fields) {
        for (String field : fields) {
            Utf8 fieldName = new Utf8(field);
            Object value = map.get(fieldName);
            Schema schema = ReflectData.get().getSchema(value.getClass());
            byte[] bytes = AvroUtils.serialize(schema, value);
            map.put(fieldName, ByteBuffer.wrap(bytes));
        }
    }

    /**
     * Encrypt fields from list
     *
     * @param list    list
     * @param indices indices to encrypt
     */
    public static void encryptList(List<Object> list, int... indices) {
        for (int index : indices) {
            Object value = list.get(index);
            Schema schema = ReflectData.get().getSchema(value.getClass());
            byte[] bytes = AvroUtils.serialize(schema, value);
            list.set(index, ByteBuffer.wrap(bytes));
        }
    }

    /**
     * Serialize records to bytes
     *
     * @param schema  schema
     * @param records records
     * @return bytes
     */
    public static byte[] serialize(Schema schema, GenericRecord... records) {
        ByteArrayOutputStream dataOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        try {
            dataFileWriter.create(schema, dataOutputStream);
            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }
            dataFileWriter.flush();
            dataFileWriter.close();
            dataOutputStream.flush();
            dataOutputStream.close();
            return dataOutputStream.toByteArray();
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Parse bytes to schema and records
     *
     * @param bytes input bytes
     * @return pair of schema and records
     */
    public static Pair<Schema, List<GenericRecord>> deserialize(byte[] bytes) {
        try {
            SeekableInput seekableInput = new SeekableByteArrayInput(bytes);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(seekableInput, datumReader);

            Schema schema = dataFileReader.getSchema();
            List<GenericRecord> records = new ArrayList<>();
            while (dataFileReader.hasNext()) {
                records.add(dataFileReader.next());
            }
            return new Pair<>(schema, records);
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Serialize record to bytes without schema
     *
     * @param schema schema
     * @param id     schema id
     * @param record record
     * @return bytes
     */
    public static byte[] serializeWithoutSchema(Schema schema, int id, GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(ID_SIZE).putInt(id).array());
            byte[] recordData = AvroUtils.serialize(schema, record);
            out.write(recordData);
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Parse bytes to schema id and record
     *
     * @param schema schema
     * @param bytes  input bytes
     * @return pair of schema id and record
     */
    public static Pair<Integer, GenericRecord> deserializeWithoutSchema(Schema schema, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.get() != MAGIC_BYTE) {
            throw new CommonException("Unknown magic byte!");
        }
        int id = buffer.getInt();
        byte[] data = new byte[buffer.limit() - buffer.position()];
        buffer.get(data);
        GenericRecord record = (GenericRecord) AvroUtils.deserialize(schema, data);
        return new Pair<>(id, record);
    }

}
