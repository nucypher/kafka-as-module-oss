package com.nucypher.kafka.clients.message

import com.nucypher.kafka.Constants
import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.Header
import com.nucypher.kafka.clients.Message
import com.nucypher.kafka.clients.decrypt.aes.AesGcmCipherDecryptor
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor
import com.nucypher.kafka.utils.AESKeyGenerators
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import com.nucypher.kafka.utils.MessageUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Specification

import java.security.Key
import java.security.PrivateKey
import java.security.PublicKey

import static com.nucypher.kafka.Constants.KEY_EDEK
import static com.nucypher.kafka.Constants.KEY_IV
import static com.nucypher.kafka.TestConstants.PEM

/**
 */
class MessageSpec extends Specification {

    static final EncryptionAlgorithm algorithm = TestConstants.ENCRYPTION_ALGORITHM

    def 'encryption and decryption message test'() {
        setup: 'initialize values'

        println "---------------------------------------------------------------"
        println " ENCRYPTION\n\n"
        DefaultProvider.initializeProvider()

        File file = new File(MessageSpec.class.getClassLoader().getResource(PEM).getFile())
        PrivateKey privateKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPrivate()
        PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        AesGcmCipherEncryptor aesGcmEncryptor = new AesGcmCipherEncryptor(algorithm, publicKey)

        String topic = "TOPIC"
        String payload = "PAYLOAD_TEXT"
        byte[] EDEK = aesGcmEncryptor.getExtraParameters().get(KEY_EDEK)
        byte[] IV = aesGcmEncryptor.getExtraParameters().get(KEY_IV)

        when: 'encrypt message'

        // for AES we need to save and IV in message
        byte[] encryptedMessageBytes = new Message<String>(
                new StringSerializer(),
                aesGcmEncryptor,
                MessageUtils.getHeader(topic, aesGcmEncryptor),
                payload)
                .encrypt()

        then: 'print out encrypted message'
        println "encryped message bytes:" + new String(encryptedMessageBytes)


        println "\n\n"
        println "---------------------------------------------------------------"
        println " DECRYPTION\n\n"


        when: 'decrypt step by step algo'

        // #1 private key
        AesGcmCipherDecryptor aesGcmDecryptor = new AesGcmCipherDecryptor(privateKey)
        Message<String> decryptedMessage = new Message<>(
                new StringDeserializer(), aesGcmDecryptor, encryptedMessageBytes)

        // #2 extract EDEK from Message bytes
        byte[] headerBytes = decryptedMessage.extractHeader(encryptedMessageBytes)
        Header decryptedHeader = new Header(headerBytes)
        byte[] dEDEK = decryptedHeader.getMap().get(KEY_EDEK)
        byte[] dIV = decryptedHeader.getMap().get(KEY_IV)

        // #3 decrypt EDEK to DEK with Private Key
        byte[] dDEK = KeyUtils.decryptEDEK(algorithm, privateKey, dEDEK, false)

        println "dDEK size:${dDEK.length} bytes:" + dDEK
        println "dEDEK size:${dEDEK.length} bytes:" + dEDEK
        println "dIV size:${dIV.length} bytes:" + dIV

        // #4 extract payload bytes
        byte[] payloadBytesNonDecrypted = decryptedMessage.extractPayload(encryptedMessageBytes)


        Key tempKey = AESKeyGenerators.create(dDEK, Constants.SYMMETRIC_ALGORITHM)

        PrivateKey _key_dDEK = new PrivateKey() {

            @Override
            String getAlgorithm() {
                return tempKey.getAlgorithm()
            }

            @Override
            String getFormat() {
                return tempKey.getFormat()
            }

            @Override
            byte[] getEncoded() {
                return tempKey.getEncoded()
            }
        }

        // #5 initialize Cipher with decrypted parameres from message DEK IV
        aesGcmDecryptor.init(_key_dDEK, dIV)

        // #6 decrypt message bytes via Aes GCM Cipher with DEK & dIV
        byte[] dPayload = aesGcmDecryptor.translate(payloadBytesNonDecrypted)

        // #7 deserialize to particular type
        String deserializedDecryptedPayLoad = new StringDeserializer().deserialize(decryptedHeader.getTopic(), dPayload)

        println "dPayload:" + dPayload
        println "deserialized & decrypted payload:" + deserializedDecryptedPayLoad


        then: 'step by step checks'
        dEDEK == EDEK
        dIV == IV
        decryptedHeader.getTopic() == topic
        deserializedDecryptedPayLoad == payload

        when: 'test decrypt method'
        String backPayload = decryptedMessage.decrypt()

        then: 'original and decrypted and deserialized payload should be equal'
        payload == backPayload

    }

}
