package com.nucypher.kafka.clients

import com.nucypher.crypto.EncryptionAlgorithm
import com.nucypher.kafka.TestUtils
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub
import com.nucypher.kafka.utils.KeyUtils
import com.nucypher.kafka.utils.SubkeyGenerator
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.KeyPair
import java.security.PrivateKey

import static TestUtils.PEM

/**
 * Test for {@link AesStructuredMessageSerializer}
 * and {@link AesStructuredMessageDeserializer}
 */
class StructuredMessageSerializeDeserializeSpec extends Specification {

    static final Class<? extends EncryptionAlgorithm> ALGORITHM =
            TestUtils.ENCRYPTION_ALGORITHM_CLASS

    def 'simple encryption and decryption'() {
        setup: 'initialization'

        String topic = "topic"
        File file = new File(this.getClass().getClassLoader()
                .getResource(PEM).getFile())
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath())

        AesStructuredMessageSerializer<String> serializer =
                new AesStructuredMessageSerializer<>(
                        new StringSerializer(),
                        ALGORITHM,
                        keyPair.public,
                        StructuredDataAccessorStub.class
                )
        AesStructuredMessageDeserializer<String> deserializer =
                new AesStructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        ALGORITHM,
                        keyPair.private,
                        StructuredDataAccessorStub.class
                )

        when: 'encrypt all fields'
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be encrypted'
        new String(bytes).matches(encryptedMessageAll)

        when: 'decrypt all fields'
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: '"a" and "b" fields should be decrypted'
        decryptedMessage == message

        when: 'encrypt only "a" field'
        serializer = new AesStructuredMessageSerializer<>(
                new StringSerializer(),
                ALGORITHM,
                keyPair.public,
                StructuredDataAccessorStub.class,
                ["a"].toSet()
        )
        bytes = serializer.serialize(topic, message)

        then: 'only "a" field should be encrypted'
        new String(bytes).matches(encryptedMessageAField)

        when: 'decrypt "a" field'
        decryptedMessage = deserializer.deserialize(topic, bytes)

        then: 'only "a" field should be decrypted'
        decryptedMessage == message

        where:
        message << ["{\"a\":\"a\", \"b\":\"b\"}",
                    "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"]
        encryptedMessageAll << ["\\{\"a\":\"\\w+\", \"b\":\"\\w+\", \"encrypted\":\\[\"a\", \"b\"]}",
                                "\\{\"a\":\"\\w+\", \"b\":\"\\w+\", \"encrypted\":\\[\"a\", \"b\"]}\n" +
                                        "\\{\"a\":\"\\w+\", \"b\":\"\\w+\", \"encrypted\":\\[\"a\", \"b\"]}"
        ]
        encryptedMessageAField << ["\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}",
                                   "\\{\"a\":\"\\w+\", \"b\":\"b\", \"encrypted\":\\[\"a\"]}\n" +
                                           "\\{\"a\":\"\\w+\", \"b\":\"d\", \"encrypted\":\\[\"a\"]}"
        ]
    }

    def 'encryption and decryption using derived keys'() {
        setup: 'initialization'
        String topic = "topic"
        String message = "{\"a\":\"a\", \"b\":\"b\"}"

        File file = new File(this.getClass().getClassLoader()
                .getResource(PEM).getFile())
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath())
        PrivateKey privateKeyA = SubkeyGenerator.deriveKey(keyPair.private, topic + "-a")

        AesStructuredMessageSerializer<String> serializer =
                new AesStructuredMessageSerializer<>(
                        new StringSerializer(),
                        ALGORITHM,
                        keyPair,
                        StructuredDataAccessorStub.class,
                        null,
                        true,
                        null
                )
        AesStructuredMessageDeserializer<String> deserializer =
                new AesStructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        ALGORITHM,
                        privateKeyA,
                        StructuredDataAccessorStub.class
                )

        when: 'encrypt fields with different keys'
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be decrypted'
        new String(bytes).matches("\\{\"a\":\"\\w+\", \"b\":\"\\w+\", \"encrypted\":\\[\"a\", \"b\"]}")

        when: 'decrypt all fields'
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: 'only "a" field should be decrypted'
        decryptedMessage.matches("\\{\"a\":\"a\", \"b\":\"\\w+\", \"encrypted\":\\[\"b\"]}")
    }
}
