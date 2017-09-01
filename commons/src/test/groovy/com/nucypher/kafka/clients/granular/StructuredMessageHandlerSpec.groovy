package com.nucypher.kafka.clients.granular

import com.nucypher.kafka.clients.EncryptedDataEncryptionKey
import com.nucypher.kafka.clients.Message
import com.nucypher.kafka.clients.MessageHandler
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager
import com.nucypher.kafka.errors.CommonException
import com.nucypher.kafka.utils.AESKeyGenerators
import com.nucypher.kafka.utils.WrapperReEncryptionKey
import org.bouncycastle.util.encoders.Hex
import spock.lang.Specification

/**
 * Test for {@link StructuredMessageHandler}
 */
class StructuredMessageHandlerSpec extends Specification {

    static final String TOPIC = "topic"
    static final DEK = AESKeyGenerators.generateDEK(32)

    def 'encryption and decryption'() {
        setup: 'initialization'

        String messageString = "{\"a\":\"a\", \"b\":\"b\"}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        DataEncryptionKeyManager keyManager = Mock()
        messageHandler.getDataEncryptionKeyManager() >> keyManager
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'encrypt structured message'
        byte[] encrypted = structuredMessageHandler
                .encrypt(TOPIC, messageBytes, dataAccessor, ["a"].toSet())

        then: 'a field should be encrypted'
        new String(encrypted).matches(".+\\{\"a\":\"\\w+\", \"b\":\"b\"}.+a.+edek")
        1 * dataAccessor.deserialize(TOPIC, messageBytes)
        1 * dataAccessor.serialize()
        1 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        1 * keyManager.getDEK(TOPIC + "-a") >> DEK
        1 * keyManager.encryptDEK(DEK, TOPIC + "-a") >> "edek".getBytes()
        1 * messageHandler.encryptMessage("a".getBytes(), DEK) >>
                new Message("c".getBytes(), "iv".getBytes())

        when: 'decrypt structured message'
        byte[] decrypted = structuredMessageHandler
                .decrypt(TOPIC, encrypted, dataAccessor)

        then: 'a field should be decrypted'
        new String(decrypted) == messageString
//        1 * dataAccessor.deserialize(TOPIC, encryptedData.getBytes())
        1 * dataAccessor.serialize()
        1 * keyManager.decryptEDEK("edek".getBytes(), false) >> DEK
        1 * messageHandler.decryptMessage(
                new Message("c".getBytes(), "iv".getBytes()), DEK) >>
                "a".getBytes()

        when: 'error while decrypt "a" field'
        decrypted = structuredMessageHandler.decrypt(TOPIC, encrypted, dataAccessor)

        then: '"a" field should not be decrypted'
        new String(decrypted).matches(
                "\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\{\"a\":\"\\w+\"}}")
        1 * keyManager.decryptEDEK("edek".getBytes(), false) >>
                { throw new CommonException() }

        when: 'encrypt "b" field in already encrypted message'
        encrypted = structuredMessageHandler
                .encrypt(TOPIC, decrypted, dataAccessor, ["b"].toSet())

        then: 'b field should be encrypted'
        new String(encrypted).matches(
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+edek.+b.+edk")
        1 * dataAccessor.deserialize(TOPIC, decrypted)
        1 * dataAccessor.serialize()
        0 * dataAccessor.getUnencrypted("a")
        1 * dataAccessor.getUnencrypted("b")
        1 * keyManager.getDEK(TOPIC + "-b") >> DEK
        1 * keyManager.encryptDEK(DEK, TOPIC + "-b") >> "edk".getBytes()
        1 * messageHandler.encryptMessage("b".getBytes(), DEK) >>
                new Message("d".getBytes(), "iv".getBytes())
    }

    def 'encryption and decryption batch message'() {
        setup: 'initialization'

        String messageString = "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"
        byte[] messageBytes = messageString.getBytes()

        StructuredDataAccessor dataAccessor = Spy(StructuredDataAccessorStub)
        MessageHandler messageHandler = Mock()
        DataEncryptionKeyManager keyManager = Mock()
        messageHandler.getDataEncryptionKeyManager() >> keyManager
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'encrypt structured message'
        byte[] bytes = structuredMessageHandler
                .encrypt(TOPIC, messageBytes, dataAccessor, ["a"].toSet())

        then: 'a field should be encrypted'
        new String(bytes).matches(".+\\{\"a\":\"\\w+\", \"b\":\"b\"}\n" +
                "\\{\"a\":\"\\w+\", \"b\":\"d\"}.+a.+edek")
        1 * dataAccessor.deserialize(TOPIC, messageBytes)
        1 * dataAccessor.serialize()
        2 * dataAccessor.getUnencrypted("a")
        0 * dataAccessor.getUnencrypted("b")
        1 * keyManager.getDEK(TOPIC + "-a") >> DEK
        1 * keyManager.encryptDEK(DEK, TOPIC + "-a") >> "edek".getBytes()
        1 * messageHandler.encryptMessage("a".getBytes(), DEK) >>
                new Message("c".getBytes(), "iv1".getBytes())
        1 * messageHandler.encryptMessage("c".getBytes(), DEK) >>
                new Message("a".getBytes(), "iv2".getBytes())

        when: 'decrypt structured message'
        byte[] decrypted = structuredMessageHandler.decrypt(
                TOPIC, bytes, dataAccessor)

        then: 'a field should be decrypted'
        new String(decrypted) == messageString
//        1 * dataAccessor.deserialize(TOPIC, bytes)
        1 * dataAccessor.serialize()
        1 * keyManager.decryptEDEK("edek".getBytes(), false) >> DEK
        1 * messageHandler.decryptMessage(
                new Message("c".getBytes(), "iv1".getBytes()), DEK) >>
                "a".getBytes()
        1 * messageHandler.decryptMessage(
                new Message("a".getBytes(), "iv2".getBytes()), DEK) >>
                "c".getBytes()

        when: 'error while decrypt "a" field'
        decrypted = structuredMessageHandler.decrypt(TOPIC, bytes, dataAccessor)

        then: '"a" field should not be decrypted'
        new String(decrypted).matches(
                "\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\{\"a\":\"\\w+\"}}\n" +
                        "\\{\"a\":\"\\w+\", \"b\":\"d\"}")
        1 * keyManager.decryptEDEK("edek".getBytes(), false) >>
                { throw new CommonException() }

        when: 'encrypt "b" field in already encrypted message'
        bytes = structuredMessageHandler
                .encrypt(TOPIC, decrypted, dataAccessor, ["b"].toSet())

        then: 'b field should be encrypted'
        new String(bytes).matches(".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}\n" +
                "\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+edek.+b.+edk")
        1 * dataAccessor.deserialize(TOPIC, decrypted)
        1 * dataAccessor.serialize()
        0 * dataAccessor.getUnencrypted("a")
        2 * dataAccessor.getUnencrypted("b")
        1 * keyManager.getDEK(TOPIC + "-b") >> DEK
        1 * keyManager.encryptDEK(DEK, TOPIC + "-b") >> "edk".getBytes()
        1 * messageHandler.encryptMessage("b".getBytes(), DEK) >>
                new Message("d".getBytes(), "iv1".getBytes())
        1 * messageHandler.encryptMessage("d".getBytes(), DEK) >>
                new Message("b".getBytes(), "iv2".getBytes())
    }

    def 're-encryption'() {
        setup: 'initialization'

        EncryptedDataEncryptionKey edek1 = new EncryptedDataEncryptionKey("edek1".getBytes())
        EncryptedDataEncryptionKey edek2 = new EncryptedDataEncryptionKey("edek2".getBytes())
        Map<String, byte[]> edeks = new HashMap<>(1)
        edeks.put("a", edek1.serialize())
        String messageString =
                "{\"a\":\"${Hex.toHexString("c".getBytes())}\", \"b\":\"b\"}}"
        byte[] messageBytes = StructuredMessageHandler.serializeMessage(
                edeks, messageString.bytes)
        edeks.put("a", edek2.serialize())
        byte[] reEncryptedMessageBytes = StructuredMessageHandler.serializeMessage(
                edeks, messageString.bytes)

        MessageHandler messageHandler = Mock()
        WrapperReEncryptionKey reKey = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'get all fields'
        Set<String> fields = structuredMessageHandler
                .getAllEncryptedFields(messageBytes)

        then: 'should be only "a" field'
        fields == ["a"].toSet()

        when: 're-encrypt empty set of fields'
        byte[] bytes = structuredMessageHandler.reEncrypt(TOPIC, [:])

        then: 'should be the same message'
        bytes == messageBytes
        0 * messageHandler.reEncryptEDEK(_, _, _)

        when: 're-encrypt "a" field'
        structuredMessageHandler
                .getAllEncryptedFields(messageBytes)
        bytes = structuredMessageHandler.reEncrypt(TOPIC, ["a": reKey])

        then: 'should be the new message'
        bytes == reEncryptedMessageBytes
        1 * messageHandler.reEncryptEDEK(TOPIC + "-a", edek1, reKey) >> edek2
    }

    def 'serialization and re-encryption batch message'() {
        setup: 'initialization'

        EncryptedDataEncryptionKey edek1 = new EncryptedDataEncryptionKey("edek1".getBytes())
        EncryptedDataEncryptionKey edek2 = new EncryptedDataEncryptionKey("edek2".getBytes())
        Map<String, byte[]> edeks = new HashMap<>(1)
        edeks.put("a", edek1.serialize())
        String messageString =
                "{\"a\":\"${Hex.toHexString("c".getBytes())}\", \"b\":\"b\"}}\n" +
                        "{\"a\":\"${Hex.toHexString("d".getBytes())}\", \"b\":\"b\"}}"
        byte[] messageBytes = StructuredMessageHandler.serializeMessage(
                edeks, messageString.bytes)
        edeks.put("a", edek2.serialize())
        byte[] reEncryptedMessageBytes = StructuredMessageHandler.serializeMessage(
                edeks, messageString.bytes)

        MessageHandler messageHandler = Mock()
        WrapperReEncryptionKey reKey = Mock()
        StructuredMessageHandler structuredMessageHandler =
                new StructuredMessageHandler(messageHandler)

        when: 'get all fields'
        Set<String> fields = structuredMessageHandler.getAllEncryptedFields(messageBytes)

        then: 'should be only "a" field'
        fields == ["a"].toSet()

        when: 're-encrypt empty set of fields'
        byte[] bytes = structuredMessageHandler.reEncrypt(TOPIC, [:])

        then: 'should be the same message'
        bytes == messageBytes
        0 * messageHandler.reEncrypt(_, _, _)

        when: 're-encrypt "a" field'
        structuredMessageHandler.getAllEncryptedFields(messageBytes)
        bytes = structuredMessageHandler.reEncrypt(TOPIC, ["a": reKey])

        then: 'should be the new message'
        bytes == reEncryptedMessageBytes
        1 * messageHandler.reEncryptEDEK(TOPIC + "-a", edek1, reKey) >> edek2
    }

}
