package com.nucypher.kafka.encrypt;

import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Key;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Collection;

import static com.nucypher.kafka.utils.KeyType.PRIVATE;
import static com.nucypher.kafka.utils.KeyType.PUBLIC;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link DataEncryptionKeyManager}
 *
 * @author szotov
 */
@RunWith(value = Parameterized.class)
public class DataEncryptionKeyManagerTest {

    private EncryptionAlgorithm algorithm;

    /**
     * @param algorithm {@link EncryptionAlgorithm}
     */
    public DataEncryptionKeyManagerTest(EncryptionAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * @return collection of {@link EncryptionAlgorithm} values
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {EncryptionAlgorithm.BBS98},
                {EncryptionAlgorithm.ELGAMAL}
        };
        return Arrays.asList(data);
    }

//    /**
//     * Test generating complex re-encryption key
//     */
//    @Test
//    //TODO move to KeyUtilsAlgorithmTest when ready
//    public void testGenerateComplexReKey() throws Exception {
//        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
//        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
//        String publicTo = getClass().getResource("/public-key-prime256v1-2.pem").getPath();
//        testGenerateComplexReKey(algorithm, privateFrom, publicTo, privateTo, "prime256v1");
//
//        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
//        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
//        publicTo = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
//        testGenerateComplexReKey(algorithm, privateFrom, publicTo, privateTo, "secp521r1");
//    }

//    private void testGenerateComplexReKey(
//            EncryptionAlgorithm algorithm,
//            String from,
//            String publicTo,
//            String privateTo,
//            String curve)
//            throws Exception {
//        KeyUtilsTest.testGenerateComplexReKey(
//                algorithm, from, publicTo, privateTo, null, DEFAULT);
//        KeyUtilsTest.testGenerateComplexReKey(
//                algorithm, from, publicTo, privateTo, curve, DEFAULT);
//        KeyUtilsTest.testGenerateComplexReKey(
//                algorithm, from, privateTo, privateTo, null, PUBLIC);
//        KeyUtilsTest.testGenerateComplexReKey(
//                algorithm, from, publicTo, privateTo, null, PRIVATE_AND_PUBLIC);
//        KeyUtilsTest.testGenerateComplexReKey(
//                algorithm, from, publicTo, privateTo, null, PUBLIC);
//    }


//    /**
//     * Test complex re-encrypting EDEK
//     */
//    @Test
//    //TODO move to KeyUtilsAlgorithmTest when ready
//    public void testComplexReEncryptEDEK() throws Exception {
//        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
//        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
//        testReEncryptEDEK(algorithm, privateFrom, privateTo, true);
//
//        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
//        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
//        testReEncryptEDEK(algorithm, privateFrom, privateTo, true);
//    }

    /**
     * Test re-encrypting EDEK
     *
     * @param algorithm encryption algorithm
     * @param from      key from
     * @param to        key to
     * @param isComplex is complex re-encryption
     */
    public static void testReEncryptEDEK(EncryptionAlgorithm algorithm,
                                         String from,
                                         String to,
                                         boolean isComplex) throws Exception {
        WrapperReEncryptionKey reKey = KeyUtils.generateReEncryptionKey(
                algorithm, from, to, !isComplex ? PRIVATE : PUBLIC, null);
        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(to);

        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm, keyPairTo.getPrivate(), keyPairFrom.getPublic());
        Key key = keyManager.getDEK("");

        byte[] encrypted = keyManager.encryptDEK(key);
        byte[] reEncrypted = keyManager.reEncryptEDEK(encrypted, reKey);
        Key decrypted = keyManager.decryptEDEK(reEncrypted, isComplex);
        assertEquals(key, decrypted);
    }

    /**
     * Test simple re-encrypting EDEK
     */
    @Test
    public void testSimpleReEncryptEDEK() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        testReEncryptEDEK(algorithm, privateFrom, privateTo, false);

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        testReEncryptEDEK(algorithm, privateFrom, privateTo, false);
    }

}
