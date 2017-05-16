package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.Pair
import com.nucypher.kafka.clients.ExtraParameters
import com.nucypher.kafka.clients.Header
import com.nucypher.kafka.clients.Message
import com.nucypher.kafka.encrypt.ByteEncryptor
import org.bouncycastle.util.encoders.Hex
import spock.lang.Specification

import static com.nucypher.kafka.Constants.KEY_EDEK
import static com.nucypher.kafka.Constants.KEY_IV

/**
 * Test for {@link StructuredMessage}
 */
class StructuredMessageSpec extends Specification {

    def 'encryption and decryption'() {
        setup: 'initialization'

        String topic = "topic"
        String messageString = "{\"a\":\"a\", \"b\":\"b\"}"
        byte[] messageBytes = messageString.getBytes()
        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)

        Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors = new HashMap<>()
        ByteEncryptor encryptorMockA = Mock()
        ExtraParameters parametersMockA = Mock()
        encryptors.put("a", new Pair<>(encryptorMockA, parametersMockA))
        Map<Object, Object> extraParametersMockA = Mock()
        parametersMockA.getExtraParameters() >> extraParametersMockA

        //TODO complete
//        DecryptorMock decryptorMockA = Mock()
//        StructuredDataAccessor dataAccessorForDecryption = Spy(StructuredDataAccessorStub)
////        decryptorMockA.getPrivateKey() >> null
////        BytesMessage messageMock = Mock()

        when: 'encrypt structured message'
        StructuredMessage message =
                new StructuredMessage(messageBytes, topic, dataAccessor, encryptors)
        byte[] bytes = message.encrypt()

        then: 'a field should be encrypted'
        new String(bytes).matches("\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}")
        1 * dataAccessor.deserialize(topic, messageBytes)
        1 * dataAccessor.serialize()
        1 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        1 * extraParametersMockA.get(KEY_EDEK)
        1 * extraParametersMockA.get(KEY_IV)
        1 * encryptorMockA.translate("a".getBytes()) >> "c".getBytes()

        //TODO complete
//        when: 'decrypt structured message'
//        message = new StructuredMessage(bytes, topic, dataAccessorForDecryption, decryptorMockA)
//        byte[] decrypted = message.decrypt()
//
//        then: 'a field should be decrypted'
//        new String(decrypted) == "{a:\"c\", b:\"b\"}"
//        1 * dataAccessor.deserialize(bytes)
//        1 * dataAccessor.serialize()
//        1 * dataAccessor.getAllEncrypted()
////        1 * decryptorMockA.translate("a".getBytes()) >> "a".getBytes()
////        1 * new BytesMessage(decryptorMockA, fieldBytes) >> messageMock
////        1 * messageMock.decrypt()
    }

    def 'encryption and decryption batch message'() {
        setup: 'initialization'

        String topic = "topic"
        String messageString = "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"
        byte[] messageBytes = messageString.getBytes()
        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)

        Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors = new HashMap<>()
        ByteEncryptor encryptorMockA = Mock()
        ExtraParameters parametersMockA = Mock()
        encryptors.put("a", new Pair<>(encryptorMockA, parametersMockA))
        Map<Object, Object> extraParametersMockA = Mock()
        parametersMockA.getExtraParameters() >> extraParametersMockA

        when: 'encrypt batch message'
        StructuredMessage message =
                new StructuredMessage(messageBytes, topic, dataAccessor, encryptors)
        byte[] bytes = message.encrypt()

        then: 'a field should be encrypted in both messages'
        new String(bytes).matches("\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}\n" +
                "\\{\"a\":\"\\w+\", \"b\":\"d\", \"encrypted\":\\[\"a\"]}")
        1 * dataAccessor.deserialize(topic, messageBytes)
        1 * dataAccessor.serialize()
        2 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        2 * extraParametersMockA.get(KEY_EDEK)
        2 * extraParametersMockA.get(KEY_IV)
        1 * encryptorMockA.translate("a".getBytes()) >> "ae".getBytes()
        1 * encryptorMockA.translate("c".getBytes()) >> "ce".getBytes()
    }

    def 'serialization and re-encryption'() {
        setup: 'initialization'

        String topic = "topic"
        Header header = Header.builder().build().add(KEY_EDEK, "a".getBytes())
        byte[] fieldMessage = Message.combine(header.serialize(), "a".getBytes())
        String messageText =
                "{\"a\":\"${Hex.toHexString(fieldMessage)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        byte[] messageBytes = messageText.getBytes()
        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)

        header = Header.builder().build().add(KEY_EDEK, "a1".getBytes())
        fieldMessage = Message.combine(header.serialize(), "a".getBytes())
        String reEncryptedMessageText =
                "{\"a\":\"${Hex.toHexString(fieldMessage)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"

        when: 'deserialize and serialize without encryption'
        StructuredMessage message = new StructuredMessage(messageBytes, topic, dataAccessor)
        byte[] bytes = message.reEncrypt()

        then: 'should be the same message'
        new String(bytes) == messageText

        when: 're-encrypt all fields'
        message = new StructuredMessage(messageBytes, topic, dataAccessor)
        for (Map.Entry<String, Header> entry : message.getAllHeaders().entrySet()) {
            message.setHeader(entry.getKey(), header)
        }
        bytes = message.reEncrypt()

        then: 'should be the new message'
        new String(bytes) == reEncryptedMessageText
    }

    def 'serialization and re-encryption batch message'() {
        setup: 'initialization'

        String topic = "topic"
        Header header = Header.builder().build().add(KEY_EDEK, "a".getBytes())
        byte[] fieldMessage1 = Message.combine(header.serialize(), "ae".getBytes())
        byte[] fieldMessage2 = Message.combine(header.serialize(), "ce".getBytes())
        String messageText =
                "{\"a\":\"${Hex.toHexString(fieldMessage1)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}\n" +
                "{\"a\":\"${Hex.toHexString(fieldMessage2)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        byte[] messageBytes = messageText.getBytes()
        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)

        header = Header.builder().build().add(KEY_EDEK, "a1".getBytes())
        fieldMessage1 = Message.combine(header.serialize(), "ae".getBytes())
        fieldMessage2 = Message.combine(header.serialize(), "ce".getBytes())
        String reEncryptedMessageText =
                "{\"a\":\"${Hex.toHexString(fieldMessage1)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}\n" +
                "{\"a\":\"${Hex.toHexString(fieldMessage2)}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"

        when: 'deserialize and serialize without encryption'
        StructuredMessage message = new StructuredMessage(messageBytes, topic, dataAccessor)
        byte[] bytes = message.reEncrypt()

        then: 'should be the same message'
        new String(bytes) == messageText

        when: 're-encrypt all fields in all lines'
        message = new StructuredMessage(messageBytes, topic, dataAccessor)
        for (Map.Entry<String, Header> entry : message.getAllHeaders().entrySet()) {
            message.setHeader(entry.getKey(), header)
        }
        bytes = message.reEncrypt()

        then: 'should be the new message'
        new String(bytes) == reEncryptedMessageText

    }

}
