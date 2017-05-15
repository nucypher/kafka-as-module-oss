package com.nucypher.kafka.clients

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.decrypt.AesMessageDecryptorDeserializer
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor
import com.nucypher.kafka.clients.encrypt.AesMessageEncryptorSerializer
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.PrivateKey
import java.security.PublicKey

import static com.nucypher.kafka.TestConstants.PEM

/**
 */
class AesMessageEncryptorDecryptorSpec extends Specification {

    static final EncryptionAlgorithm algorithm = TestConstants.ENCRYPTION_ALGORITHM

    static {
        DefaultProvider.initializeProvider()
    }

    def 'encrypt and decrypt message'() {
        setup: 'initialize'

        String data = "1234567890QWERTY1234567890QWERTY1234567890QWERTY"

        // load PEM file from resources
        File file = new File(this.getClass().getClassLoader().getResource(PEM).getFile())
        PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate()
        PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        // initialize Encryptor
        AesGcmCipherEncryptor aesGcmEncryptor = new AesGcmCipherEncryptor(algorithm, publicKey)
        AesMessageEncryptorSerializer<String> messageEncryptorSerializer =
                new AesMessageEncryptorSerializer<>(
                        new StringSerializer(),
                        aesGcmEncryptor,
                        aesGcmEncryptor
                )

        AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey)
        AesMessageDecryptorDeserializer<String> messageDecryptorDeserializer =
                new AesMessageDecryptorDeserializer<>(
                        new StringDeserializer(),
                        aesGcmDecryptor
                )


        when: 'encrypt to byte array'
        byte[] encrypted = messageEncryptorSerializer.serialize("", data)



        String result = messageDecryptorDeserializer.deserialize("", encrypted)

        then: 'get decrypted result from deserializer'
        result == data
    }

}
