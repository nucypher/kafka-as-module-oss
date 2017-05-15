package com.nucypher.kafka.clients.encrypt

import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

/**
 *
 */
class ByteEncryptorSerializerSpec extends Specification {

	def 'byte serializer with empty topic'() {
		setup: 'payload is String'

		ByteEncryptorSerializer<String> byteEncryptorSerializer = new ByteEncryptorSerializer<String>(new StringSerializer())
		String payload = "PAYLOAD"

		when: 'serialize'
		byte[] serialized = byteEncryptorSerializer.serialize("", payload)

		then: 'should be equal to original payload'
		payload == new String(serialized)

	}
}
