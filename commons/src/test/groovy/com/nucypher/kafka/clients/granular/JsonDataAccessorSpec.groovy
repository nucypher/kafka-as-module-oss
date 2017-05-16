package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.errors.CommonException
import org.bouncycastle.util.encoders.Hex
import spock.lang.Specification

/**
 * Test for {@link JsonDataAccessor}
 */
class JsonDataAccessorSpec extends Specification {

    private static String complexObjectJson = "{" +
            "\"a\":[false,true]," +
            "\"b\":{\"b\":10}," +
            "\"c\":\"c\"," +
            "\"d\":null," +
            "\"e\":{\"e\":{\"e\":[{\"e\":\"e\"}]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"1\"}"

    private static String partiallyEncryptedComplexObjectJson = "{" +
            "\"a\":[false,true]," +
            "\"b\":{\"b\":10}," +
            "\"c\":\"${Hex.toHexString("\"c\"".getBytes())}\"," +
            "\"d\":null," +
            "\"e\":{\"e\":{\"e\":[{\"e\":\"e\"}]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"1\"," +
            "\"encrypted\":[\"c\"]}"

    private static String encryptedComplexObjectJson = "{" +
            "\"a\":[\"${Hex.toHexString("false".getBytes())}\",true]," +
            "\"b\":{\"b\":\"${Hex.toHexString("10".getBytes())}\"}," +
            "\"c\":\"${Hex.toHexString("\"c\"".getBytes())}\"," +
            "\"d\":\"${Hex.toHexString("null".getBytes())}\"," +
            "\"e\":{\"e\":{\"e\":[\"${Hex.toHexString("{\"e\":\"e\"}".getBytes())}\"]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"${Hex.toHexString("\"1\"".getBytes())}\"," +
            "\"encrypted\":[\"c\",\"a.1\",\"b.b\",\"d\",\"e.e.e.1\",\"1\"]}"

    private static String arrayJson = "[{\"a\":1},2]"

    private static String encryptedArrayJson =
            "[{\"a\":\"${Hex.toHexString("1".getBytes())}\"}," +
                    "\"${Hex.toHexString("2".getBytes())}\"]"

    def 'serialization and deserialization'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()

        when: 'deserialize and serialize json'
        dataAccessor.deserialize(null, json.getBytes())
        byte[] bytes = dataAccessor.serialize()

        then: 'should be the same data'
        new String(bytes) == json

        where:
        json << [complexObjectJson, encryptedComplexObjectJson, arrayJson, encryptedArrayJson]
    }

    def 'getting all fields'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()

        when: 'get all fields from complex json'
        dataAccessor.deserialize(null, complexObjectJson.getBytes())

        then: 'should be "a.1", "a.2", "b.b", "c", "d", "e.e.e.1.e", "z", "1"'
        dataAccessor.getAllFields() ==
                ["a.1", "a.2", "b.b", "c", "d", "e.e.e.1.e", "z", "1"].toSet()

        when: 'get all fields from array json'
        dataAccessor.deserialize(null, arrayJson.getBytes())

        then: 'should be "1.a", "2"'
        dataAccessor.getAllFields() == ["1.a", "2"].toSet()
    }

    def 'getting value by field name'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, complexObjectJson.getBytes())

        when: 'get value of each field from complex json'
        byte[] c = dataAccessor.getUnencrypted("c")
        byte[] bb = dataAccessor.getUnencrypted("b.b")
        byte[] a2 = dataAccessor.getUnencrypted("a.2")
        byte[] d = dataAccessor.getUnencrypted("d")
        byte[] eee1 = dataAccessor.getUnencrypted("e.e.e.1")
        byte[] f1 = dataAccessor.getUnencrypted("1")

        then: 'should be right values'
        new String(c) == "\"c\""
        new String(bb) == "10"
        new String(a2) == "true"
        new String(d) == "null"
        new String(eee1) == "{\"e\":\"e\"}"
        new String(f1) == "\"1\""

        when: 'get value of nonexistent field'
        dataAccessor.getUnencrypted("a.3")

        then: 'thrown not-found exception'
        thrown(CommonException)

        when: 'get value of nonexistent field'
        dataAccessor.getUnencrypted("z.z")

        then: 'thrown not-found exception'
        thrown(CommonException)

        when: 'get value of each field from array json'
        dataAccessor.deserialize(null, arrayJson.getBytes())
        byte[] first = dataAccessor.getUnencrypted("1")
        byte[] second = dataAccessor.getUnencrypted("2")
        byte[] a = dataAccessor.getUnencrypted("1.a")

        then: 'should be right values'
        new String(first) == "{\"a\":1}"
        new String(second) == "2"
        new String(a) == "1"
    }

    def 'getting all encrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, encryptedComplexObjectJson.getBytes())

        when: 'get all encrypted values from complex json'
        Map<String, byte[]> values = dataAccessor.getAllEncrypted()

        then: 'should be right values'
        values == ["1":"\"1\"".getBytes(),
                   "a.1":"false".getBytes(),
                   "b.b":"10".getBytes(),
                   "c":"\"c\"".getBytes(),
                   "d":"null".getBytes(),
                   "e.e.e.1":"{\"e\":\"e\"}".getBytes()]

        when: 'get all encrypted values from array json'
        dataAccessor.deserialize(null, encryptedArrayJson.getBytes())
        values = dataAccessor.getAllEncrypted()

        then: 'should be right values'
        values == ["1.a":"1".getBytes(),
                   "2":"2".getBytes()]

        when: 'get all encrypted values from unencrypted complex json'
        dataAccessor.deserialize(null, complexObjectJson.getBytes())
        values = dataAccessor.getAllEncrypted()

        then: 'should be 0 values'
        values.size() == 0
    }

    def 'adding unencrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, encryptedComplexObjectJson.getBytes())

        when: 'add some unencrypted values to complex json'
        dataAccessor.addUnencrypted("a.1", "false".getBytes())
        dataAccessor.addUnencrypted("b.b", "10".getBytes())
        dataAccessor.addUnencrypted("d", "null".getBytes())
        dataAccessor.addUnencrypted("e.e.e.1", "{\"e\":\"e\"}".getBytes())
        dataAccessor.addUnencrypted("1", "\"1\"".getBytes())
        byte[] bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectJson

        when: 'add rest unencrypted values to complex json'
        dataAccessor.addUnencrypted("c", "\"c\"".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be unencrypted json'
        new String(bytes) == complexObjectJson

        when: 'add all unencrypted values to array json'
        dataAccessor.deserialize(null, encryptedArrayJson.getBytes())
        dataAccessor.addUnencrypted("1.a", "1".getBytes())
        dataAccessor.addUnencrypted("2", "2".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be unencrypted json'
        new String(bytes) == arrayJson
    }

    def 'adding encrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, complexObjectJson.getBytes())

        when: 'add some encrypted values to complex json'
        dataAccessor.addEncrypted("c", "\"c\"".getBytes())
        byte[] bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectJson

        when: 'add rest encrypted values to complex json'
        dataAccessor.addEncrypted("a.1", "false".getBytes())
        dataAccessor.addEncrypted("b.b", "10".getBytes())
        dataAccessor.addEncrypted("d", "null".getBytes())
        dataAccessor.addEncrypted("e.e.e.1", "{\"e\":\"e\"}".getBytes())
        dataAccessor.addEncrypted("1", "\"1\"".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be encrypted json'
        new String(bytes) == encryptedComplexObjectJson

        when: 'add all encrypted values to array json'
        dataAccessor.deserialize(null, arrayJson.getBytes())
        dataAccessor.addEncrypted("1.a", "1".getBytes())
        dataAccessor.addEncrypted("2", "2".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be encrypted json'
        new String(bytes) == encryptedArrayJson
    }

    def 'getting next element'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()

        when: 'deserialize complex data'
        dataAccessor.deserialize(null, json.getBytes())

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
        json << [complexObjectJson, arrayJson]
    }

}
