package com.nucypher.kafka.cipher

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.utils.AESKeyGenerators
import spock.lang.Specification

import java.security.Key

/**
 * Test for {@link AesGcmCipher}
 */
class AesGcmCipherSpec extends Specification {

    def 'encrypt and decrypt data with AES GCM DEK'() {
        setup: 'initialize'
        Key key = AESKeyGenerators.generateDEK(32)
        Random random = new Random()
        byte[] originalData = new byte[1024]
        random.nextBytes(originalData)
        byte[] iv = new byte[originalData.length]
        random.nextBytes(iv)
        AesGcmCipher cipher = new AesGcmCipher()

        when: 'encrypt and decrypt data'
        byte[] encryptedData = cipher.encrypt(originalData, key, iv)
        byte[] decryptedData = cipher.decrypt(encryptedData, key, iv)

        then: 'compare original and decrypted data'
        decryptedData == originalData
    }

}
