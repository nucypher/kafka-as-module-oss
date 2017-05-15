package com.nucypher.kafka.clients

import spock.lang.Specification

/**
 *
 */
class MessageBytesSpec extends Specification {

    def 'convert'(){
        setup: 'init'

        byte[] header = "header".getBytes()
        byte[] payload = "payload".getBytes()

        when: 'create from header & payload'
        MessageBytes messageBytes = new MessageBytes().setHeader(header).setPayload(payload)

        byte[] bytes = messageBytes.toByteArray()

        MessageBytes backMessageBytes = new MessageBytes().fromByteArray(bytes)

        then: 'compare'
        println "bytes:" + bytes
        println "back:" + new String(bytes)

        println ""
        println "back header:" + new String(backMessageBytes.getHeader())
        println "back payload:" + new String(backMessageBytes.getPayload())

        header == backMessageBytes.getHeader()
        payload == backMessageBytes.getPayload()

    }
}
