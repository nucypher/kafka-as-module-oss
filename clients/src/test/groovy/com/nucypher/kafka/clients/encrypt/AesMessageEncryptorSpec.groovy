package com.nucypher.kafka.clients.encrypt

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.clients.Header
import com.nucypher.kafka.clients.Message
import com.nucypher.kafka.clients.MessageBytes
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor
import com.nucypher.kafka.utils.ByteUtils
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.Ignore
import spock.lang.Specification

import java.security.PublicKey

import static com.nucypher.kafka.TestConstants.PEM
import static com.nucypher.kafka.utils.ByteUtils.hex

/**
 */
class AesMessageEncryptorSpec extends Specification {

    static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM

    static {
        DefaultProvider.initializeProvider()
    }

    def 'encrypt message'() {
        setup: 'initialize'

        String data = "1234567890QWERTY"

        // load PEM file from resources
        File file = new File(AesMessageEncryptorSpec.class.getClassLoader().getResource(PEM).getFile())
        PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        // initialize Encryptor
        AesGcmCipherEncryptor aesGcmEncryptor = new AesGcmCipherEncryptor(ALGORITHM, publicKey)

        AesMessageEncryptorSerializer<String> messageEncryptorSerializer =
                new AesMessageEncryptorSerializer<>(
                        new StringSerializer(),
                        aesGcmEncryptor,
                        aesGcmEncryptor
                )

        when: 'encrypt to byte array'
        byte[] encrypted  = messageEncryptorSerializer.serialize("TOPIC NAME", data)

        MessageBytes messageBytes = new MessageBytes().fromByteArray(encrypted)
        byte[] headerBytes = messageBytes.getHeader()

        println "header:" + Header.deserialize(headerBytes)



        then: 'compare and print out'
        println "original HEX:" + hex(data.getBytes())
        println "encrypted HEX:" + hex(encrypted)

        println "just to look inside that topic is not encrypted"
        println "original String:" + new String(data.getBytes())
        println "encrypted String:" + new String(encrypted)
    }

    @Ignore
    def 'encrypt message - unable deserialize encrypted payload java.io.StreamCorruptedException'() {
        setup: 'initialize'

        String data = "1234567890QWERTY"

        // load PEM file from resources
        File file = new File(AesMessageEncryptorSpec.class.getClassLoader().getResource(PEM).getFile())
        PublicKey publicKey = KeyUtils.getECKeyPairFromPEM(file.getAbsolutePath()).getPublic()

        // initialize Encryptor
        AesGcmCipherEncryptor aesGcmEncryptor = new AesGcmCipherEncryptor(ALGORITHM, publicKey)

        AesMessageEncryptorSerializer messageEncryptorSerializer = new AesMessageEncryptorSerializer<String>(
                new StringSerializer(),
                aesGcmEncryptor,
                aesGcmEncryptor
        );

        when: 'encrypt to byte array'
        byte[] encrypted  = messageEncryptorSerializer.serialize("TOPIC NAME", data)

        MessageBytes messageBytes = new MessageBytes().fromByteArray(encrypted)
        byte[] headerBytes = messageBytes.getHeader()
        byte[] payloadBytes = messageBytes.getPayload()

        println "header:" + (Header)ByteUtils.deserialize(headerBytes)

        (Message)ByteUtils.deserialize(payloadBytes)

        then: 'exception'
        thrown(StreamCorruptedException)
    }

}
