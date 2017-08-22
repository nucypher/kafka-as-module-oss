package com.nucypher.kafka.clients

import spock.lang.Specification

/**
 * Test for {@link Message}
 */
class MessageSpec extends Specification {

    def 'test serialization and deserialization'() {
        setup: 'initialize parameters'

        Random random = new Random()
        byte[] edek = "123456789".getBytes()
        byte[] iv = "123456789".getBytes()
        byte[] payload = new byte[1024]
        random.nextBytes(payload)

        when: 'serialize and deserialize message'
        Message message = new Message(payload, iv, new EncryptedDataEncryptionKey(edek))
        byte[] serialized = message.serialize()
        message = Message.deserialize(serialized)

        then: 'compare deserialized with original data'
        message.payload == payload
        message.EDEK.bytes == edek
        message.IV == iv
        !message.EDEK.complex
    }
}
