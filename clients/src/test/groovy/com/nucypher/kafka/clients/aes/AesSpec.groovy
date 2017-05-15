package com.nucypher.kafka.clients.aes

import com.nucypher.kafka.DefaultProvider
import com.nucypher.kafka.TestConstants
import com.nucypher.kafka.utils.AESKeyGenerators
import com.nucypher.kafka.utils.EncryptionAlgorithm
import com.nucypher.kafka.utils.KeyUtils
import spock.lang.Ignore
import spock.lang.Specification

import javax.crypto.SecretKey
import java.security.*

import static com.nucypher.kafka.TestConstants.PEM
import static com.nucypher.kafka.utils.ByteUtils.hex
import static com.nucypher.kafka.utils.KeyType.PRIVATE_AND_PUBLIC

/**
 */
class AesSpec extends Specification {

    static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM

    static {
        DefaultProvider.initializeProvider()
    }

    def 'encrypt DEK to EDEK with Public key'() {
        setup: 'initialize'

        Key DEK = AESKeyGenerators.generateDEK()

        File file = new File(getClass().getClassLoader().getResource(PEM).getFile())
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(file.absolutePath)
        PublicKey publicKey = keyPair.getPublic()
        PrivateKey privateKey = keyPair.getPrivate()

        when: 'encrypt DEK'
        println "DEK length:" + DEK.encoded.length
        byte[] EDEK = KeyUtils.encryptDEK(ALGORITHM, publicKey, DEK, new SecureRandom())

        int keyLength = DEK.getEncoded().length
        println "keyLength:$keyLength"

        println "DEK:" + DEK.getEncoded()
        println "EDEK:" + EDEK

        byte[] backDEK = KeyUtils.decryptEDEK(ALGORITHM, privateKey, EDEK, false)

        then: 'compare decrypted and original'
        backDEK == DEK.getEncoded()
    }

    def 'test key length'() {
        setup: 'initialize'

        Key key128 = AESKeyGenerators.AES_128.generateKey()
        Key key192 = AESKeyGenerators.AES_192.generateKey()
        Key key256 = AESKeyGenerators.AES_256.generateKey()

        when: 'print out '
        println "key128 size:" + key128.getEncoded().length + ":" + hex(key128.getEncoded())
        println "key192 size:" + key192.getEncoded().length + ":" + hex(key192.getEncoded())
        println "key256 size:" + key256.getEncoded().length + ":" + hex(key256.getEncoded())

        then: 'compare key size in bits'
        128 == 8 * key128.getEncoded().length
        192 == 8 * key192.getEncoded().length
        256 == 8 * key256.getEncoded().length


        when: 'generate another key'
        Key key256_2 = AESKeyGenerators.AES_256.generateKey()

        then: 'new key is not equal'
        key256.getEncoded() != key256_2.getEncoded()

    }

    def 'load keypair from PEM file'() {
        setup: ''
        File file = new File(getClass().getClassLoader().getResource(PEM).getFile())

        when: 'load from file'
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(file.absolutePath)

        println "private size:" + keyPair.getPrivate().getEncoded().length + " :" + hex(keyPair.getPrivate().getEncoded())
        println "public size:" + keyPair.getPublic().getEncoded().length + ":" + hex(keyPair.getPublic().getEncoded())

        then: ''
        hex(keyPair.getPrivate().getEncoded()) == "3081F7020100301006072A8648CE3D020106052B810400230481DF3081DC0201010442007F3CC85BE009853B66152A77AAA2D66A32880E3D5C62874F69CD0660EF24BB6D7B4CD532E8A99758399CDE7637B2755FDBD1B671975BF0953D687B3FF9BE78D5A6A00706052B81040023A181890381860004015E1D54F25A46150000A6DB18FC38CE68E1892A4E0CEAE1E4ACD204FB927BA841BD1D452DCB2ADC1F83CF61ECB440DD52A6464DFCA07750967D09DD97E37CCFBCA20142D63FA5D1B15099B36A0D26A7D876AA2E86EB7CCC3FD63E09B33E619300A3404A1418E947048255654D1064BDC1C855D47B6A2E18D5C5BC7A56FED3E0A1F4D1F5"
        hex(keyPair.getPublic().getEncoded()) == "30819B301006072A8648CE3D020106052B810400230381860004015E1D54F25A46150000A6DB18FC38CE68E1892A4E0CEAE1E4ACD204FB927BA841BD1D452DCB2ADC1F83CF61ECB440DD52A6464DFCA07750967D09DD97E37CCFBCA20142D63FA5D1B15099B36A0D26A7D876AA2E86EB7CCC3FD63E09B33E619300A3404A1418E947048255654D1064BDC1C855D47B6A2E18D5C5BC7A56FED3E0A1F4D1F5"
    }

    @Ignore
    def 'test generate PEM file with different curves'() {
        setup: 'initialize'



        when: 'prime256v1'

        KeyPair keyPair = KeyUtils.generateECKeyPair(ALGORITHM, "secp521r1").getFirst()

        println "private size:" + keyPair.getPrivate().getEncoded().length + " :" +
                hex(keyPair.getPrivate().getEncoded())
        println "public size:" + keyPair.getPublic().getEncoded().length + ":" +
                hex(keyPair.getPublic().getEncoded())

        then: 'print out prime256v1'
        for (int i = 0; i < 10; i++) {
            SecretKey secretKey = KeyUtils.AESKeyGenerators.AES_256.generateKey()
            System.out.println("AES keys i" + i + ":" +
                    hex(secretKey.getEncoded()) + " size:" + secretKey.getEncoded().length / 16)
        }

        KeyUtils.generateECKeyPairToPEM(ALGORITHM, TestConstants.PEM, "secp521r1", PRIVATE_AND_PUBLIC)

        when: 'secp521r1'

        keyPair = KeyUtils.generateECKeyPair(ALGORITHM, "secp521r1").getFirst()

        println "private size:" + keyPair.getPrivate().getEncoded().length + " :" +
                hex(keyPair.getPrivate().getEncoded())
        println "public size:" + keyPair.getPublic().getEncoded().length + ":" +
                hex(keyPair.getPublic().getEncoded())


        then: 'print out secp521r1'

        for (int i = 0; i < 10; i++) {
            SecretKey secretKey = AESKeyGenerators.AES_256.generateKey()
            System.out.println("AES keys i" + i + ":" + hex(secretKey.getEncoded()) + " size:" +
                    secretKey.getEncoded().length / 16)
        }

        KeyUtils.generateECKeyPairToPEM(ALGORITHM, PEM, "secp521r1", PRIVATE_AND_PUBLIC)
    }
}
