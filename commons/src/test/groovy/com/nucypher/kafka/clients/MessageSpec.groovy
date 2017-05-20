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
        byte[] EDEK = "123456789".getBytes()
        byte[] IV = "123456789".getBytes()
        byte[] payload = new byte[1024]
        random.nextBytes(payload)

        Map<String, byte[]> compareMap = new HashMap<>()
        compareMap.put(KEY_EDEK, EDEK)
        compareMap.put(KEY_IV, IV)

        when: 'serialize and deserialize message'

        Header header = new Header(topic)
                .add(KEY_EDEK, EDEK)
                .add(KEY_IV, IV)
        Message message = new Message(header, payload)
        byte[] serialized = message.serialize()
        message = Message.deserialize(serialized)
        header = message.header

        then: 'compare deserialized with original data'

        topic == header.getTopic()
        header.getMap() != null
        compareMap.keySet() == header.getMap().keySet()
        compareMap == header.getMap()
        message.payload == payload
    }
}
