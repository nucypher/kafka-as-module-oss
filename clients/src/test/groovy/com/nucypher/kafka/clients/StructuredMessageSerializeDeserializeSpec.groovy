package com.nucypher.kafka.clients

import com.nucypher.crypto.EncryptionAlgorithm
import com.nucypher.kafka.TestUtils
import com.nucypher.kafka.clients.decrypt.AesMessageDeserializerConfig
import com.nucypher.kafka.clients.decrypt.AesStructuredMessageDeserializer
import com.nucypher.kafka.clients.encrypt.AesMessageSerializerConfig
import com.nucypher.kafka.clients.encrypt.AesStructuredMessageSerializer
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub
import com.nucypher.kafka.utils.KeyUtils
import com.nucypher.kafka.utils.SubkeyGenerator
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.KeyPair
import java.security.PrivateKey
import java.util.regex.Pattern

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
                        null,
                        StructuredDataAccessorStub.class
                )
        AesStructuredMessageDeserializer<String> deserializer =
                new AesStructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        ALGORITHM,
                        keyPair.private,
                        StructuredDataAccessorStub.class
                )
        Pattern allPattern = Pattern.compile(encryptedMessageAll, Pattern.DOTALL)
        Pattern aFieldPattern = Pattern.compile(encryptedMessageAField, Pattern.DOTALL)

        when: 'encrypt all fields'
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be encrypted'
        allPattern.matcher(new String(bytes)).matches()

        when: 'decrypt all fields'
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: '"a" and "b" fields should be decrypted'
        decryptedMessage == message

        when: 'encrypt only "a" field'
        serializer = new AesStructuredMessageSerializer<>(
                new StringSerializer(),
                ALGORITHM,
                keyPair.public,
                null,
                StructuredDataAccessorStub.class,
                ["a"].toSet()
        )
        bytes = serializer.serialize(topic, message)

        then: 'only "a" field should be encrypted'
        aFieldPattern.matcher(new String(bytes)).matches()

        when: 'decrypt "a" field'
        decryptedMessage = deserializer.deserialize(topic, bytes)

        then: 'only "a" field should be decrypted'
        decryptedMessage == message

        where:
        message << ["{\"a\":\"a\", \"b\":\"b\"}",
                    "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"]
        encryptedMessageAll << [
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+b.+",
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}\n" +
                        "\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+b.+"
        ]
        encryptedMessageAField << [
                ".+\\{\"a\":\"\\w+\", \"b\":\"b\"}.+a.+",
                ".+\\{\"a\":\"\\w+\", \"b\":\"b\"}\n" +
                        "\\{\"a\":\"\\w+\", \"b\":\"d\"}.+a.+"
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
                        null,
                        StructuredDataAccessorStub.class,
                        null,
                        true,
                        null,
                        null,
                        null
                )
        AesStructuredMessageDeserializer<String> deserializer =
                new AesStructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        ALGORITHM,
                        privateKeyA,
                        StructuredDataAccessorStub.class
                )
        Pattern allPattern = Pattern.compile(
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+b.+", Pattern.DOTALL)

        when: 'encrypt fields with different keys'
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be decrypted'
        allPattern.matcher(new String(bytes)).matches()

        when: 'decrypt all fields'
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: 'only "a" field should be decrypted'
        decryptedMessage.matches(
                "\\{\"a\":\"a\", \"b\":\"\\w+\", \"encrypted\":\\{\"b\":\"\\w+\"}}")
    }

    def 'serializer/deserializer initialization using properties'() {
        setup: 'initialization'

        String topic = "topic"
        File file = new File(this.getClass().getClassLoader()
                .getResource(PEM).getFile())

        AesStructuredMessageSerializer<String> serializer =
                new AesStructuredMessageSerializer()
        Map<String, ?> configs = new HashMap<>()
        configs.put(MessageSerDeConfig.DEK_ENCRYPTION_ALGORITHM_CONFIG, ALGORITHM.getCanonicalName())
        configs.put(AesMessageSerializerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getCanonicalName())
        configs.put(AesMessageSerializerConfig.PUBLIC_KEY_CONFIG,
                file.getAbsolutePath())
        configs.put(StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                StructuredDataAccessorStub.class.getCanonicalName())
        serializer.configure(configs, false)

        AesStructuredMessageDeserializer<String> deserializer =
                new AesStructuredMessageDeserializer<>()
        configs = new HashMap<>()
        configs.put(MessageSerDeConfig.DEK_ENCRYPTION_ALGORITHM_CONFIG, ALGORITHM.getCanonicalName())
            configs.put(AesMessageDeserializerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName())
        configs.put(AesMessageDeserializerConfig.PRIVATE_KEY_CONFIG,
                file.getAbsolutePath())
        configs.put(StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG,
                StructuredDataAccessorStub.class.getCanonicalName())
        deserializer.configure(configs, false)

        Pattern allPattern = Pattern.compile(encryptedMessageAll, Pattern.DOTALL)

        when: 'encrypt all fields'
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be encrypted'
        allPattern.matcher(new String(bytes)).matches()

        when: 'decrypt all fields'
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: '"a" and "b" fields should be decrypted'
        decryptedMessage == message

        where:
        message << ["{\"a\":\"a\", \"b\":\"b\"}",
                    "{\"a\":\"a\", \"b\":\"b\"}\n{\"a\":\"c\", \"b\":\"d\"}"]
        encryptedMessageAll << [
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+b.+",
                ".+\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}\n" +
                        "\\{\"a\":\"\\w+\", \"b\":\"\\w+\"}.+a.+b.+"
        ]
    }
}
