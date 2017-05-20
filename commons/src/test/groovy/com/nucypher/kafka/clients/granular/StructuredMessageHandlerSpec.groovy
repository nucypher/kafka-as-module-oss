package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.clients.MessageHandler
import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.WrapperReEncryptionKey
import org.bouncycastle.util.encoders.Hex
import spock.lang.Specification

/**
 * Test for {@link StructuredMessageHandler}
 */
class StructuredMessageHandlerSpec extends Specification {

    static final String TOPIC = "topic"

    def 'encryption and decryption'() {
        setup: 'initialization'

        String messageString = "{\"a\":\"a\", \"b\":\"b\"}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'encrypt structured message'
        byte[] encrypted = structuredMessageHandler
                .encrypt(TOPIC, messageBytes, dataAccessor, ["a"].toSet())

        then: 'a field should be encrypted'
        new String(encrypted).matches("\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}")
        1 * dataAccessor.deserialize(TOPIC, messageBytes)
        1 * dataAccessor.serialize()
        1 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        1 * messageHandler.encrypt(TOPIC + "-a", "a".getBytes()) >> "c".getBytes()

        when: 'decrypt structured message'
        byte[] decrypted = structuredMessageHandler
                .decrypt(TOPIC, encrypted, dataAccessor)

        then: 'a field should be decrypted'
        new String(decrypted) == messageString
        1 * dataAccessor.deserialize(TOPIC, encrypted)
        1 * dataAccessor.serialize()
        1 * messageHandler.decrypt("c".getBytes()) >> "a".getBytes()

        when: 'error while decrypt "a" field'
        decrypted = structuredMessageHandler.decrypt(TOPIC, encrypted, dataAccessor)

        then: '"a" field should not be decrypted'
        decrypted == encrypted
        messageHandler.decrypt("c".getBytes()) >> { throw new CommonException() }
    }

    def 'encryption and decryption batch message'() {
        setup: 'initialization'

        String messageString = "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'encrypt structured message'
        byte[] bytes = structuredMessageHandler
                .encrypt(TOPIC, messageBytes, dataAccessor, ["a"].toSet())

        then: 'a field should be encrypted'
        new String(bytes).matches("\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}\n" +
                "\\{\"a\":\"\\w+\", \"b\":\"d\", \"encrypted\":\\[\"a\"]}")
        1 * dataAccessor.deserialize(TOPIC, messageBytes)
        1 * dataAccessor.serialize()
        2 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        1 * messageHandler.encrypt(TOPIC + "-a", "a".getBytes()) >> "c".getBytes()
        1 * messageHandler.encrypt(TOPIC + "-a", "c".getBytes()) >> "a".getBytes()

        when: 'decrypt structured message'
        byte[] decrypted = bytes = structuredMessageHandler
                .decrypt(TOPIC, bytes, dataAccessor)

        then: 'a field should be decrypted'
        new String(decrypted) == messageString
        1 * dataAccessor.deserialize(TOPIC, bytes)
        1 * dataAccessor.serialize()
        1 * messageHandler.decrypt("c".getBytes()) >> "a".getBytes()
        1 * messageHandler.decrypt("a".getBytes()) >> "c".getBytes()
    }

    def 're-encryption'() {
        setup: 'initialization'

        String messageString =
                "{\"a\":\"${Hex.toHexString("c".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        String reEncryptedMessageString =
                "{\"a\":\"${Hex.toHexString("d".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        WrapperReEncryptionKey reKey = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'get all fields'
        Set<String> fields = structuredMessageHandler
                .getAllEncrypted(TOPIC, messageBytes, dataAccessor)

        then: 'should be only "a" field'
        fields == ["a"].toSet()

        when: 're-encrypt empty set of fields'
        byte[] bytes = structuredMessageHandler.reEncrypt([:])

        then: 'should be the same message'
        new String(bytes) == messageString
        0 * messageHandler.reEncrypt(_, _)

        when: 're-encrypt "a" field'
        structuredMessageHandler
                .getAllEncrypted(TOPIC, messageBytes, dataAccessor)
        bytes = structuredMessageHandler.reEncrypt("a": reKey)

        then: 'should be the new message'
        new String(bytes) == reEncryptedMessageString
        1 * dataAccessor.deserialize(TOPIC, bytes)
        1 * dataAccessor.serialize()
        1 * messageHandler.reEncrypt("c".getBytes(), reKey) >> "d".getBytes()
    }

    def 'serialization and re-encryption batch message'() {
        setup: 'initialization'

        String messageString =
                "{\"a\":\"${Hex.toHexString("c".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}\n" +
                        "{\"a\":\"${Hex.toHexString("d".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        String reEncryptedMessageString =
                "{\"a\":\"${Hex.toHexString("d".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}\n" +
                        "{\"a\":\"${Hex.toHexString("e".getBytes())}\", \"b\":\"b\", \"encrypted\":[\"a\"]}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        WrapperReEncryptionKey reKey = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'get all fields'
        Set<String> fields = structuredMessageHandler
                .getAllEncrypted(TOPIC, messageBytes, dataAccessor)

        then: 'should be only "a" field'
        fields == ["a"].toSet()

        when: 're-encrypt empty set of fields'
        byte[] bytes = structuredMessageHandler.reEncrypt([:])

        then: 'should be the same message'
        new String(bytes) == messageString
        0 * messageHandler.reEncrypt(_, _)

        when: 're-encrypt "a" field'
        structuredMessageHandler
                .getAllEncrypted(TOPIC, messageBytes, dataAccessor)
        bytes = structuredMessageHandler.reEncrypt("a": reKey)

        then: 'should be the new message'
        new String(bytes) == reEncryptedMessageString
        1 * dataAccessor.deserialize(TOPIC, bytes)
        1 * dataAccessor.serialize()
        1 * messageHandler.reEncrypt("c".getBytes(), reKey) >> "d".getBytes()
        1 * messageHandler.reEncrypt("d".getBytes(), reKey) >> "e".getBytes()
    }

}
