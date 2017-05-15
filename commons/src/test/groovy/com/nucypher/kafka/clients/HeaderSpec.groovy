package com.nucypher.kafka.clients

import spock.lang.Specification

import static com.nucypher.kafka.Constants.KEY_EDEK
import static com.nucypher.kafka.Constants.KEY_IV

/**
 * Test for {@link Header}
 */
class HeaderSpec extends Specification {

    def 'test serialization and deserialization'() {
        setup: 'initialize parameters'

        String topic = "TOPIC"
        String description = "DESCRIPTION"
        byte[] EDEK = "123456789".getBytes()
        byte[] IV = "123456789".getBytes()

        Map<String, byte[]> compareMap = new HashMap<>()
        compareMap.put(KEY_EDEK, EDEK)
        compareMap.put(KEY_IV, IV)

        when: 'add to header extra parameters'

        Header header = new Header(topic, description, null)
                .add(KEY_EDEK, EDEK)
                .add(KEY_IV, IV)

        byte[] serialized = header.serialize()

        then: ' look-through serialized bytes and find some patterns'
        def string = new String(serialized)
        println "serialized string:" + string
        string.contains("TOPIC")

        when: 'deserialize Header from byte array'
        Header backHeader = new Header(serialized)

        then: 'compare deserialized with original data'

        topic == backHeader.getTopic()
        description == backHeader.getDescription()
        backHeader.getMap() != null
        compareMap.keySet() == backHeader.getMap().keySet()
        compareMap == backHeader.getMap()

    }
}
