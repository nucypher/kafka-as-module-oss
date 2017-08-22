package com.nucypher.kafka.clients

import com.nucypher.kafka.cipher.AbstractCipher
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager
import com.nucypher.kafka.utils.AESKeyGenerators
import com.nucypher.kafka.utils.WrapperReEncryptionKey
import spock.lang.Specification

import java.security.Key

/**
 * Test for {@link MessageHandler}
 */
class MessageHandlerSpec extends Specification {

    static final DEK = AESKeyGenerators.generateDEK(32)

    def 'test encryption'() {
        setup: 'initialize'
        Random random = new Random()
        byte[] data = new byte[1024]
        random.nextBytes(data)
        String topic = "TOPIC"
        Key key = DEK

        DataEncryptionKeyManager keyManager = Mock()
        AbstractCipher cipher = Mock()
        MessageHandler messageHandler = new MessageHandler(cipher, keyManager)

        when: 'encrypt message'
        byte[] serialized = messageHandler.encrypt(topic, data)
        Message message = Message.deserialize(serialized)

        then: 'should be message object'
        message.payload == data
        message.iv != null
        message.EDEK.bytes == key.getEncoded()
        1 * keyManager.getDEK(topic) >> key
        1 * keyManager.encryptDEK(key, topic) >> key.getEncoded()
        1 * cipher.encrypt(data, key, _) >> data
    }

    def 'test decryption'() {
        setup: 'initialize'
        Random random = new Random()
        byte[] data = new byte[1024]
        random.nextBytes(data)
        byte[] iv = new byte[data.length]
        random.nextBytes(iv)
        Key key = DEK
        Message message = new Message(
                data, iv, new EncryptedDataEncryptionKey(key.getEncoded()))

        DataEncryptionKeyManager keyManager = Mock()
        AbstractCipher cipher = Mock()
        MessageHandler messageHandler = new MessageHandler(cipher, keyManager)

        when: 'decrypt message'
        byte[] decrypted = messageHandler.decrypt(message.serialize())

        then: 'should be initial data'
        decrypted == data
        1 * keyManager.decryptEDEK(key.getEncoded(), false) >> key
        1 * cipher.decrypt(data, key, iv) >> data
    }

    def 'test re-encryption'() {
        setup: 'initialize'
        Random random = new Random()
        byte[] data = new byte[1024]
        random.nextBytes(data)
        byte[] iv = new byte[data.length]
        random.nextBytes(iv)
        String topic = "TOPIC"
        Key key = DEK
        Message message = new Message(
                data, iv, new EncryptedDataEncryptionKey(key.getEncoded()))

        DataEncryptionKeyManager keyManager = Mock()
        WrapperReEncryptionKey reKey = Mock()
        MessageHandler messageHandler = new MessageHandler(keyManager)

        when: 'simple re-encrypt message'
        byte[] reEncrypted = messageHandler.reEncrypt(topic, message.serialize(), reKey)
        message = Message.deserialize(reEncrypted)

        then: 'should be right message object'
        message.payload == data
        message.iv == iv
        message.EDEK.bytes == key.getEncoded()
        !message.EDEK.isComplex()
        1 * keyManager.reEncryptEDEK(topic, key.getEncoded(), reKey) >> key.getEncoded()

        when: 'complex re-encrypt message'
        reEncrypted = messageHandler.reEncrypt(topic, message.serialize(), reKey)
        message = Message.deserialize(reEncrypted)

        then: 'should be right message object'
        message.payload == data
        message.iv == iv
        message.EDEK.bytes == key.getEncoded()
        message.EDEK.isComplex()
        keyManager.reEncryptEDEK(topic, key.getEncoded(), reKey) >> key.getEncoded()
        reKey.isComplex() >> true
    }
}
