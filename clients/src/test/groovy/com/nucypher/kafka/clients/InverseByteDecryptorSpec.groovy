package com.nucypher.kafka.clients

import com.nucypher.kafka.clients.decrypt.InverseByteDecryptor
import com.nucypher.kafka.errors.client.DecryptorException
import com.nucypher.kafka.utils.ByteUtils
import spock.lang.Specification

/**
 *
 */
class InverseByteDecryptorSpec extends Specification {

    def 'test inverse byte decryptor'() {
        setup: 'initial states'

        String string = "qwerty"
        byte[] correctBytes = string.bytes
        def inverseByteDecryptor = new InverseByteDecryptor()

        when: 'invert bytes'
        def result = inverseByteDecryptor.translate(correctBytes)

        then: 'get correct inverted result'
        result == ByteUtils.invert(string.bytes)


        when: 'null bytes'
        inverseByteDecryptor.translate(null)

        then:
        thrown DecryptorException
    }
}
