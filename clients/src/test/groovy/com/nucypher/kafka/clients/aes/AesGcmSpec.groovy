package com.nucypher.kafka.clients.aes

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import spock.lang.Specification

import java.security.PublicKey

import static com.nucypher.kafka.utils.ByteUtils.hex

/**
 */
class AesGcmSpec extends Specification {

    static final EncryptionAlgorithm algorithm = TestConstants.ENCRYPTION_ALGORITHM

    static {
        DefaultProvider.initializeProvider()
    }

    def 'encrypt data with AES GCM DEK'(){
        setup: 'initialize'

        File file = new File(AesGcmSpec.class.getClassLoader()
                .getResource(TestConstants.PEM).getFile())
        final PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        byte[] originalData = "1234567890".getBytes()

        when: 'encrypt data'

        byte[] encryptedData = new AesGcmCipherEncryptor(algorithm, publicKey).translate(originalData)

        println "original:" + hex(originalData)
        println "encrypted:" + hex(encryptedData)

        then: 'compare original and encrypted'
        encryptedData != originalData
    }

}
