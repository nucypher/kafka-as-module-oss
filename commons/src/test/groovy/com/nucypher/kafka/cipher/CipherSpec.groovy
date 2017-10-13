package com.nucypher.kafka.cipher

import com.nucypher.kafka.utils.AESKeyGenerators
import spock.lang.Shared
import spock.lang.Specification

import java.security.Key

/**
 * Test for ciphers
 */
class CipherSpec extends Specification {

    @Shared
    byte[] originalData
    @Shared
    byte[] iv

    def setupSpec() {
        Random random = new Random()
        originalData = new byte[1024]
        random.nextBytes(originalData)
        iv = new byte[16]
        random.nextBytes(iv)
    }

    def 'encrypt and decrypt data using BouncyCastle'() {
        setup: 'initialize'
        Key key = AESKeyGenerators.generateDEK(keySize)
        ICipher cipher = CipherFactory.getCipher(
                CipherFactory.CipherProvider.BOUNCY_CASTLE,
                transformation)

        when: 'encrypt and decrypt data'
        byte[] encryptedData = cipher.encrypt(originalData, key, iv)
        byte[] decryptedData = cipher.decrypt(encryptedData, key, iv)

        then: 'compare original and decrypted data'
        decryptedData == originalData

        where:
        [keySize, transformation] << [
                [16, 24, 32],
                ["AES/GCM/NoPadding",
                 "AES/CBC/PKCS5Padding",
                 "AES/CBC/PKCS7Padding"]].combinations()
    }

    def 'encrypt and decrypt data using OpenSSL'() {
        setup: 'initialize'
        Key key = AESKeyGenerators.generateDEK(keySize)
        ICipher cipher = CipherFactory.getCipher(
                CipherFactory.CipherProvider.OPENSSL,
                transformation)

        when: 'encrypt and decrypt data'
        byte[] encryptedData = cipher.encrypt(originalData, key, iv)
        byte[] decryptedData = cipher.decrypt(encryptedData, key, iv)

        then: 'compare original and decrypted data'
        decryptedData == originalData

        where:
        [keySize, transformation] << [
                [16, 24, 32],
//                ["AES/GCM/NoPadding",
//                 "AES/CBC/PKCS5Padding",
//                 "AES/CBC/PKCS7Padding"]
                ["AES/CBC/PKCS5Padding"]].combinations()
    }

}
