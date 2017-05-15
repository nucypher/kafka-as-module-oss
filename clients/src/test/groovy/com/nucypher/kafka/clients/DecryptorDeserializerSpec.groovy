package com.nucypher.kafka.clients

import com.nucypher.kafka.clients.decrypt.ByteDecryptorDeserializer
import com.nucypher.kafka.clients.decrypt.InverseByteDecryptor
import com.nucypher.kafka.clients.decrypt.StringByteDecryptorDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import spock.lang.Specification

import static com.nucypher.kafka.utils.ByteUtils.invert


/**
 */
class DecryptorDeserializerSpec extends Specification {

    def 'test consumer decryptor and deserializer from Kafka broker'() {
        setup: 'initialize decryptorDeserializer'

        def decryptorDeserializer = new ByteDecryptorDeserializer<String>(
                new StringDeserializer(),
                new InverseByteDecryptor()
        );

        when: 'emulate data from Kafka broker'
        byte[] encryptedBytesFromKafkaBroker = invert("123".getBytes())

        then: 'get decrypted result from broker'
        "123" == decryptorDeserializer.deserialize("TOPIC", encryptedBytesFromKafkaBroker)


        when: 'initialize StringByteDecryptorDeserializer'
        StringByteDecryptorDeserializer stringDecryptorDeserializer = new StringByteDecryptorDeserializer(new InverseByteDecryptor())

        then: 'get decrypted String from Kafka broker'
        "123" == stringDecryptorDeserializer.deserialize("TOPIC", encryptedBytesFromKafkaBroker)

    }
}
