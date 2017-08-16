package com.nucypher.kafka.clients

import spock.lang.Specification

import static com.nucypher.kafka.Constants.KEY_EDEK
import static com.nucypher.kafka.Constants.KEY_IV

/**
 * Test for {@link Header}
 */
class MessageSpec extends Specification {

    def 'test serialization and deserialization'() {
        setup: 'initialize parameters'

        Random random = new Random()
        String topic = "TOPIC"
        byte[] edek = "123456789".getBytes()
        byte[] iv = "123456789".getBytes()
        byte[] payload = new byte[1024]
        random.nextBytes(payload)

        when: 'serialize and deserialize message'
        Message message = new Message(payload, topic, edek, iv)
        byte[] serialized = message.serialize()
        message = Message.deserialize(serialized)

        then: 'compare deserialized with original data'
        message.topic == topic
        message.payload == payload
        message.EDEK == edek
        message.IV == iv
        !message.complex
    }
}
