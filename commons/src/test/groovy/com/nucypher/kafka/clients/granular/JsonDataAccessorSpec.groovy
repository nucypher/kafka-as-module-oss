package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.errors.CommonException
import org.bouncycastle.util.encoders.Base64
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
            "\"c\":\"${Base64.toBase64String("\"c\"".getBytes())}\"," +
            "\"d\":null," +
            "\"e\":{\"e\":{\"e\":[{\"e\":\"e\"}]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"1\"," +
            "\"edeks\":{\"c\":\"${Base64.toBase64String("edek-c".getBytes())}\"}}"

    private static String partiallyEncryptedComplexObjectNoEDEKsJson = "{" +
            "\"a\":[false,true]," +
            "\"b\":{\"b\":10}," +
            "\"c\":\"${Base64.toBase64String("\"c\"".getBytes())}\"," +
            "\"d\":null," +
            "\"e\":{\"e\":{\"e\":[{\"e\":\"e\"}]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"1\"}"

    private static String encryptedComplexObjectJson = "{" +
            "\"a\":[\"${Base64.toBase64String("false".getBytes())}\",true]," +
            "\"b\":{\"b\":\"${Base64.toBase64String("10".getBytes())}\"}," +
            "\"c\":\"${Base64.toBase64String("\"c\"".getBytes())}\"," +
            "\"d\":\"${Base64.toBase64String("null".getBytes())}\"," +
            "\"e\":{\"e\":{\"e\":[\"${Base64.toBase64String("{\"e\":\"e\"}".getBytes())}\"]}}," +
            "\"z\":\"z\"," +
            "\"1\":\"${Base64.toBase64String("\"1\"".getBytes())}\"," +
            "\"edeks\":{" +
            "\"c\":\"${Base64.toBase64String("edek-c".getBytes())}\"," +
            "\"a.1\":\"${Base64.toBase64String("edek-a.1".getBytes())}\"," +
            "\"b.b\":\"${Base64.toBase64String("edek-b.b".getBytes())}\"," +
            "\"d\":\"${Base64.toBase64String("edek-d".getBytes())}\"," +
            "\"e.e.e.1\":\"${Base64.toBase64String("edek-e.e.e.1".getBytes())}\"," +
            "\"1\":\"${Base64.toBase64String("edek-1".getBytes())}\"}}"

//    private static String arrayJson = "[{\"a\":1},2]"
//
//    private static String encryptedArrayJson =
//            "[{\"a\":{\"edek\":\"${Base64.toBase64String("edek-1.a".getBytes())}\"," +
//                    "\"data\":\"${Base64.toBase64String("1".getBytes())}\"}}," +
//                    "{\"edek\":\"${Base64.toBase64String("edek-2".getBytes())}\"," +
//                    "\"data\":\"${Base64.toBase64String("2".getBytes())}\"}]"

    def 'serialization and deserialization'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()

        when: 'deserialize and serialize json'
        dataAccessor.deserialize(null, json.getBytes())
        byte[] bytes = dataAccessor.serialize()

        then: 'should be the same data'
        new String(bytes) == json

        where:
        json << [complexObjectJson, encryptedComplexObjectJson]
        //, arrayJson, encryptedArrayJson]
    }

    def 'getting all fields'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()

        when: 'get all fields from complex json'
        dataAccessor.deserialize(null, complexObjectJson.getBytes())

        then: 'should be "a.1", "a.2", "b.b", "c", "d", "e.e.e.1.e", "z", "1"'
        dataAccessor.getAllFields() ==
                ["a.1", "a.2", "b.b", "c", "d", "e.e.e.1.e", "z", "1"].toSet()

//        when: 'get all fields from array json'
//        dataAccessor.deserialize(null, arrayJson.getBytes())
//
//        then: 'should be "1.a", "2"'
//        dataAccessor.getAllFields() == ["1.a", "2"].toSet()
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

//        when: 'get value of each field from array json'
//        dataAccessor.deserialize(null, arrayJson.getBytes())
//        byte[] first = dataAccessor.getUnencrypted("1")
//        byte[] second = dataAccessor.getUnencrypted("2")
//        byte[] a = dataAccessor.getUnencrypted("1.a")
//
//        then: 'should be right values'
//        new String(first) == "{\"a\":1}"
//        new String(second) == "2"
//        new String(a) == "1"
    }

    def 'getting encrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, encryptedComplexObjectJson.getBytes())

        when: 'get all encrypted values from complex json'
        byte[] c = dataAccessor.getEncrypted("c")
        byte[] bb = dataAccessor.getEncrypted("b.b")
        byte[] a1 = dataAccessor.getEncrypted("a.1")
        byte[] d = dataAccessor.getEncrypted("d")
        byte[] eee1 = dataAccessor.getEncrypted("e.e.e.1")
        byte[] f1 = dataAccessor.getEncrypted("1")

        then: 'should be right values'
        new String(c) == "\"c\""
        new String(bb) == "10"
        new String(a1) == "false"
        new String(d) == "null"
        new String(eee1) == "{\"e\":\"e\"}"
        new String(f1) == "\"1\""

        when: 'get value of nonexistent field'
        dataAccessor.getEncrypted("a.3")

        then: 'thrown not-found exception'
        thrown(CommonException)

        when: 'get value of nonexistent field'
        dataAccessor.getEncrypted("z.z")

        then: 'thrown not-found exception'
        thrown(CommonException)

//        when: 'get all encrypted values from array json'
//        dataAccessor.deserialize(null, encryptedArrayJson.getBytes())
//        byte[] second = dataAccessor.getEncrypted("2")
//        byte[] a = dataAccessor.getEncrypted("1.a")
//
//        then: 'should be right values'
//        new String(a) == "1"
//        new String(second) == "2"
    }

    def 'getting all EDEKs'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, encryptedComplexObjectJson.getBytes())

        when: 'get all encrypted values from complex json'
        Map<String, byte[]> values = dataAccessor.getAllEDEKs()

        then: 'should be right values'
        values == ["1"      : "edek-1".getBytes(),
                   "a.1"    : "edek-a.1".getBytes(),
                   "b.b"    : "edek-b.b".getBytes(),
                   "c"      : "edek-c".getBytes(),
                   "d"      : "edek-d".getBytes(),
                   "e.e.e.1": "edek-e.e.e.1".getBytes()]

//        when: 'get all encrypted values from array json'
//        dataAccessor.deserialize(null, encryptedArrayJson.getBytes())
//        values = dataAccessor.getAllEDEKs()
//
//        then: 'should be right values'
//        values == ["1.a": "edek-1.a".getBytes(),
//                   "2"  : "edek-2".getBytes()]

        when: 'get all encrypted values from unencrypted complex json'
        dataAccessor.deserialize(null, complexObjectJson.getBytes())
        values = dataAccessor.getAllEDEKs()

        then: 'should be 0 values'
        values.size() == 0
    }

    def 'adding unencrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, encryptedComplexObjectJson.getBytes())

        when: 'add some unencrypted values to complex json'
        dataAccessor.addUnencrypted("a.1", "false".getBytes())
        dataAccessor.removeEDEK("a.1")
        dataAccessor.addUnencrypted("b.b", "10".getBytes())
        dataAccessor.removeEDEK("b.b")
        dataAccessor.addUnencrypted("d", "null".getBytes())
        dataAccessor.removeEDEK("d")
        dataAccessor.addUnencrypted("e.e.e.1", "{\"e\":\"e\"}".getBytes())
        dataAccessor.removeEDEK("e.e.e.1")
        dataAccessor.addUnencrypted("1", "\"1\"".getBytes())
        dataAccessor.removeEDEK("1")
        byte[] bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectJson

        when: 'remove EDEK'
        dataAccessor.removeEDEK("c")
        bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectNoEDEKsJson

        when: 'add rest unencrypted values to complex json'
        dataAccessor.addUnencrypted("c", "\"c\"".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be unencrypted json'
        new String(bytes) == complexObjectJson

//        when: 'add all unencrypted values to array json'
//        dataAccessor.deserialize(null, encryptedArrayJson.getBytes())
//        dataAccessor.addUnencrypted("1.a", "1".getBytes())
//        dataAccessor.addUnencrypted("2", "2".getBytes())
//        bytes = dataAccessor.serialize()
//
//        then: 'should be unencrypted json'
//        new String(bytes) == arrayJson
    }

    def 'adding encrypted values'() {
        setup: 'initialization'
        JsonDataAccessor dataAccessor = new JsonDataAccessor()
        dataAccessor.deserialize(null, complexObjectJson.getBytes())

        when: 'add some encrypted values to complex json'
        dataAccessor.addEncrypted("c", "\"c\"".getBytes())
        byte[] bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectNoEDEKsJson

        when: 'add EDEK to complex json'
        dataAccessor.addEDEK("c", "edek-c".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be right json'
        new String(bytes) == partiallyEncryptedComplexObjectJson

        when: 'add rest encrypted values to complex json'
        dataAccessor.addEncrypted("a.1", "false".getBytes())
        dataAccessor.addEncrypted("b.b", "10".getBytes())
        dataAccessor.addEncrypted("d", "null".getBytes())
        dataAccessor.addEncrypted("e.e.e.1", "{\"e\":\"e\"}".getBytes())
        dataAccessor.addEncrypted("1", "\"1\"".getBytes())
        dataAccessor.addEDEK("a.1", "edek-a.1".getBytes())
        dataAccessor.addEDEK("b.b", "edek-b.b".getBytes())
        dataAccessor.addEDEK("d", "edek-d".getBytes())
        dataAccessor.addEDEK("e.e.e.1", "edek-e.e.e.1".getBytes())
        dataAccessor.addEDEK("1", "edek-1".getBytes())
        bytes = dataAccessor.serialize()

        then: 'should be encrypted json'
        new String(bytes) == encryptedComplexObjectJson

//        when: 'add all encrypted values to array json'
//        dataAccessor.deserialize(null, arrayJson.getBytes())
//        dataAccessor.addEncrypted("1.a", "1".getBytes())
//        dataAccessor.addEncrypted("2", "2".getBytes())
//        dataAccessor.addEDEK("1.a", "edek-1.a".getBytes())
//        dataAccessor.addEDEK("2", "edek-2".getBytes())
//        bytes = dataAccessor.serialize()
//
//        then: 'should be encrypted json'
//        new String(bytes) == encryptedArrayJson
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
        json << [complexObjectJson]//, arrayJson]
    }

}
