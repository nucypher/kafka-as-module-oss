package com.nucypher.kafka.clients

import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.decrypt.AesMessageDeserializer
import com.nucypher.kafka.clients.encrypt.AesMessageSerializer
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.KeyPair

import static com.nucypher.kafka.TestConstants.PEM

/**
 * Test for {@link AesMessageSerializer} and {@link AesMessageDeserializer}
 */
class AesMessageEncryptorDecryptorSpec extends Specification {

    static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM

    def 'encrypt and decrypt message'() {
        setup: 'initialization'

        String topic = "topic"
        Random random = new Random()
        String data = new BigInteger(130, random).toString(32)

        File file = new File(this.getClass().getClassLoader()
                .getResource(PEM).getFile())
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath())

        AesMessageSerializer<String> messageSerializer =
                new AesMessageSerializer<>(
                        new StringSerializer(),
                        ALGORITHM,
                        keyPair.public
                )

        AesMessageDeserializer<String> messageDeserializer =
                new AesMessageDeserializer<>(
                        new StringDeserializer(),
                        ALGORITHM,
                        keyPair.private
                )

        when: 'encrypt and decrypt'
        byte[] encrypted = messageSerializer.serialize(topic, data)
        String result = messageDeserializer.deserialize(topic, encrypted)

        then: 'should be initial data'
        result == data
    }

}
