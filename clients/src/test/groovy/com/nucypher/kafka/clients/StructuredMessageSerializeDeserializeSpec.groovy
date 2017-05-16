package com.nucypher.kafka.clients

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.Pair
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.decrypt.StructuredMessageDeserializer
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor
import com.nucypher.kafka.clients.encrypt.StructuredMessageSerializer
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptorFactory
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub
import com.nucypher.kafka.encrypt.ByteEncryptor
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.PrivateKey
import java.security.PublicKey

import static com.nucypher.kafka.TestConstants.PEM

/**
 * Test for StructuredMessage, StructuredMessageSerializer and StructuredMessageDeserializer
 */
class StructuredMessageSerializeDeserializeSpec extends Specification {

    static final EncryptionAlgorithm algorithm = TestConstants.ENCRYPTION_ALGORITHM

    def 'simple encryption and decryption'() {
        setup: 'initialization'

        DefaultProvider.initializeProvider()
        String topic = "topic"

        File file = new File(this.getClass().getClassLoader().getResource(PEM).getFile())
        PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate()
        PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        when: 'encrypt all fields'
        AesGcmCipherEncryptorFactory aesGcmEncryptorFactory =
                new AesGcmCipherEncryptorFactory(algorithm, publicKey)
        StructuredMessageSerializer<String> serializer =
                new StructuredMessageSerializer<>(
                        new StringSerializer(),
                        aesGcmEncryptorFactory,
                        StructuredDataAccessorStub.class
                )
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be encrypted'
        new String(bytes).matches(encryptedMessageAll)

        when: 'decrypt all fields'
        AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey)
        StructuredMessageDeserializer deserializer =
                new StructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        aesGcmDecryptor,
                        StructuredDataAccessorStub.class
                )
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: '"a" and "b" fields should be decrypted'
        decryptedMessage == message

        when: 'encrypt only "a" field'
        serializer = new StructuredMessageSerializer<>(
                new StringSerializer(),
                ["a"],
                aesGcmEncryptorFactory,
                StructuredDataAccessorStub.class
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

    def 'encryption and decryption with different keys'() {

        setup: 'initialization'

        DefaultProvider.initializeProvider()
        String topic = "topic"
        String message = "{\"a\":\"a\", \"b\":\"b\"}"

        File file = new File(this.getClass().getClassLoader().getResource(PEM).getFile())
        PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate()
        PublicKey publicKey1 = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()
        PublicKey publicKey2 = KeyUtils.generateECKeyPair(algorithm, "P-521").getFirst().getPublic()
        Map<String, Pair<ByteEncryptor, ExtraParameters>> encryptors = new HashMap<>()
        AesGcmCipherEncryptor encryptor1 = new AesGcmCipherEncryptor(algorithm, publicKey1)
        AesGcmCipherEncryptor encryptor2 = new AesGcmCipherEncryptor(algorithm, publicKey2)
        encryptors.put("a", new Pair<ByteEncryptor, ExtraParameters>(encryptor1, encryptor1))
        encryptors.put("b", new Pair<ByteEncryptor, ExtraParameters>(encryptor2, encryptor2))

        when: 'encrypt fields with different keys'
        StructuredMessageSerializer<String> serializer =
                new StructuredMessageSerializer<>(
                        new StringSerializer(),
                        encryptors,
                        StructuredDataAccessorStub.class
                )
        byte[] bytes = serializer.serialize(topic, message)

        then: '"a" and "b" fields should be decrypted'
        new String(bytes).matches("\\{\"a\":\"\\w+\", \"b\":\"\\w+\", \"encrypted\":\\[\"a\", \"b\"]}")

        when: 'decrypt all fields'
        AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey)
        StructuredMessageDeserializer deserializer =
                new StructuredMessageDeserializer<>(
                        new StringDeserializer(),
                        aesGcmDecryptor,
                        StructuredDataAccessorStub.class
                )
        String decryptedMessage = deserializer.deserialize(topic, bytes)

        then: 'only "a" field should be decrypted'
        decryptedMessage.matches("\\{\"a\":\"a\", \"b\":\"\\w+\", \"encrypted\":\\[\"b\"]}")
    }
}
