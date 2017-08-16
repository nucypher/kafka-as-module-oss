package com.nucypher.kafka.clients

import com.nucypher.kafka.cipher.AbstractCipher
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager
import com.nucypher.kafka.utils.AESKeyGenerators
import com.nucypher.kafka.utils.ByteUtils
import com.nucypher.kafka.utils.WrapperReEncryptionKey
import spock.lang.Specification

import java.security.Key

import static com.nucypher.kafka.Constants.KEY_EDEK
import static com.nucypher.kafka.Constants.KEY_IS_COMPLEX
import static com.nucypher.kafka.Constants.KEY_IV

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
        message.topic == topic
        message.getIV() != null
        message.getEDEK() == key.getEncoded()
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
        String topic = "TOPIC"
        Key key = DEK
        Message message = new Message(data, topic, key.getEncoded(), iv)

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
        Message message = new Message(data, topic, key.getEncoded(), iv)

        DataEncryptionKeyManager keyManager = Mock()
        WrapperReEncryptionKey reKey = Mock()
        MessageHandler messageHandler = new MessageHandler(keyManager)

        when: 'simple re-encrypt message'
        byte[] reEncrypted = messageHandler.reEncrypt(message.serialize(), reKey)
        message = Message.deserialize(reEncrypted)

        then: 'should be right message object'
        message.payload == data
        message.topic == topic
        message.getIV() == iv
        message.getEDEK() == key.getEncoded()
        !message.isComplex()
        1 * keyManager.reEncryptEDEK(topic, key.getEncoded(), reKey) >> key.getEncoded()

        when: 'complex re-encrypt message'
        reEncrypted = messageHandler.reEncrypt(message.serialize(), reKey)
        message = Message.deserialize(reEncrypted)

        then: 'should be right message object'
        message.payload == data
        message.topic == topic
        message.getIV() == iv
        message.getEDEK() == key.getEncoded()
        message.isComplex()
        keyManager.reEncryptEDEK(topic, key.getEncoded(), reKey) >> key.getEncoded()
        reKey.isComplex() >> true
    }
}
