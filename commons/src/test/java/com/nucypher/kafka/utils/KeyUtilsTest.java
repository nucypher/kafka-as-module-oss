package com.nucypher.kafka.utils;

import com.nucypher.crypto.bbs98.WrapperBBS98;
import com.nucypher.crypto.elgamal.WrapperElGamalPRE;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.hamcrest.core.StringContains;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.util.Random;

import static com.nucypher.kafka.utils.KeyType.DEFAULT;
import static com.nucypher.kafka.utils.KeyType.PRIVATE;
import static com.nucypher.kafka.utils.KeyType.PRIVATE_AND_PUBLIC;
import static com.nucypher.kafka.utils.KeyType.PUBLIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for {@link KeyUtils}
 *
 * @author szotov
 */
public final class KeyUtilsTest {

    private static final EncryptionAlgorithm algorithm = TestConstants.ENCRYPTION_ALGORITHM;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static Random random = new Random();

    /**
     * Provider initialization
     */
    @BeforeClass
    public static void initialize() {
        DefaultProvider.initializeProvider();
    }

    /**
     * Test reading PEM file with EC private key
     */
    @Test
    public void testReadPEMFile() throws IOException {
        String filename = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        ECParameterSpec ecSpec = ECNamedCurveTable.getParameterSpec("prime256v1");
        ECPrivateKey key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filename).getPrivate();
        assertNotNull(key);
        assertNotNull(key.getParameters());
        assertEquals(ecSpec, key.getParameters());

        filename = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        ecSpec = ECNamedCurveTable.getParameterSpec("secp521r1");
        key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filename).getPrivate();
        assertNotNull(key);
        assertNotNull(key.getParameters());
        assertEquals(ecSpec, key.getParameters());

        filename = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        ecSpec = ECNamedCurveTable.getParameterSpec("prime256v1");
        key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filename).getPrivate();
        assertNotNull(key);
        assertNotNull(key.getParameters());
        assertEquals(ecSpec, key.getParameters());

        filename = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        ecSpec = ECNamedCurveTable.getParameterSpec("secp521r1");
        key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filename).getPrivate();
        assertNotNull(key);
        assertNotNull(key.getParameters());
        assertEquals(ecSpec, key.getParameters());

        filename = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        ecSpec = ECNamedCurveTable.getParameterSpec("secp521r1");
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(filename);
        assertNull(keyPair.getPrivate());
        ECPublicKey publicKey = (ECPublicKey) keyPair.getPublic();
        assertNotNull(publicKey);
        assertNotNull(publicKey.getParameters());
        assertEquals(ecSpec, publicKey.getParameters());

        filename = getClass().getResource("/public-key-prime256v1-2.pem").getPath();
        ecSpec = ECNamedCurveTable.getParameterSpec("prime256v1");
        keyPair = KeyUtils.getECKeyPairFromPEM(filename);
        assertNull(keyPair.getPrivate());
        publicKey = (ECPublicKey) keyPair.getPublic();
        assertNotNull(publicKey);
        assertNotNull(publicKey.getParameters());
        assertEquals(ecSpec, publicKey.getParameters());
    }

    /**
     * Test generating re-encryption key with different EC parameters
     */
    @Test
    public void testGenerateReKeyWithDifferentParameters1() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage("Different EC parameters");
        String from = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String to = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        testGenerateSimpleReKey(algorithm, from, to, null);
    }

    /**
     * Test generating re-encryption key with different EC parameters
     */
    @Test
    public void testGenerateReKeyWithDifferentParameters2() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage("Different EC parameters");
        String from = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String to = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        testGenerateSimpleReKey(algorithm, from, to, "secp521r1");
    }

    /**
     * Test generating re-encryption key with different EC parameters
     */
    @Test
    public void testGenerateReKeyWithDifferentParameters3() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage("Different EC parameters");
        String from = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        String to = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        testGenerateSimpleReKey(algorithm, from, to, "secp521r1");
    }

    /**
     * Test generating re-encryption key with wrong key type
     */
    @Test
    public void testGenerateReKeyWithWrongKeyType1() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("must contain private key"));
        String from = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        String to = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        testGenerateSimpleReKey(algorithm, from, to, null);
    }

//    /**
//     * Test generating re-encryption key with wrong key type
//     */
//    @Test
//    public void testGenerateReKeyWithWrongKeyType2() throws Exception {
//        expectedException.expect(CommonException.class);
//        expectedException.expectMessage(StringContains.containsString("must contain private key"));
//        String from = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
//        String to = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
//        testGenerateComplexReKey(algorithm, from, to, null, null, PRIVATE);
//    }

    /**
     * Test generating simple re-encryption key
     *
     * @param algorithm encryption algorithm
     * @param from      key from
     * @param to        key to
     * @param curve     curve name
     */
    public static void testGenerateSimpleReKey(
            EncryptionAlgorithm algorithm, String from, String to, String curve) throws Exception {
        WrapperReEncryptionKey reKey =
                KeyUtils.generateReEncryptionKey(algorithm, from, to, DEFAULT, null);
        WrapperReEncryptionKey reKey1 =
                KeyUtils.generateReEncryptionKey(algorithm, from, to, DEFAULT, curve);
        WrapperReEncryptionKey reKey2 =
                KeyUtils.generateReEncryptionKey(algorithm, from, to, PRIVATE, null);
        WrapperReEncryptionKey reKey3 =
                KeyUtils.generateReEncryptionKey(algorithm, from, to, PRIVATE_AND_PUBLIC, null);
        assertEquals(reKey, reKey1);
        assertEquals(reKey, reKey2);
        assertEquals(reKey, reKey3);

        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(to);
        ECParameterSpec ecSpec = getEcParameterSpec(curve, reKey, keyPairFrom);

        byte[] byteContent;
        byte[] encrypted;
        byte[] reEncrypted;
        byte[] decrypted;
        switch (algorithm) {
            case BBS98:
                byteContent = new byte[WrapperBBS98.getMessageLength(ecSpec)];
                random.nextBytes(byteContent);

                WrapperBBS98 wrapperBBS98 = new WrapperBBS98(ecSpec, new SecureRandom());
                encrypted = wrapperBBS98.encrypt(keyPairFrom.getPublic(), byteContent);
                reEncrypted = wrapperBBS98.reencrypt(reKey.getReEncryptionKey(), encrypted);
                decrypted = wrapperBBS98.decrypt(keyPairTo.getPrivate(), reEncrypted);
                break;
            case ELGAMAL:
                byteContent = new byte[WrapperElGamalPRE.getMessageLength(ecSpec)];
                random.nextBytes(byteContent);

                WrapperElGamalPRE wrapperElGamal =
                        new WrapperElGamalPRE(ecSpec, new SecureRandom());
                encrypted = wrapperElGamal.encrypt(keyPairFrom.getPublic(), byteContent);
                reEncrypted = wrapperElGamal.reencrypt(reKey.getReEncryptionKey(), encrypted);
                decrypted = wrapperElGamal.decrypt(keyPairTo.getPrivate(), reEncrypted);
                break;
            default:
                throw new CommonException();
        }
        assertArrayEquals(byteContent, decrypted);
    }

//    /**
//     * Test generating complex re-encryption key
//     *
//     * @param algorithm encryption algorithm
//     * @param from      key from
//     * @param to        key to
//     * @param privateTo private key to
//     * @param curve     curve name
//     * @param keyType   key type
//     */
//    public static void testGenerateComplexReKey(
//            EncryptionAlgorithm algorithm,
//            String from,
//            String to,
//            String privateTo,
//            String curve,
//            KeyType keyType)
//            throws Exception {
//        WrapperReEncryptionKey reKey = KeyUtils.generateReEncryptionKey(
//                algorithm, from, to, keyType, curve);
//
//        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
//        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(privateTo);
//        ECParameterSpec ecSpec = getEcParameterSpec(curve, reKey, keyPairFrom);
//
//        switch (algorithm) {
//            default:
//                //TODO complete
//                throw new CommonException();
//        }
//    }

    /**
     * Get EC parameters
     *
     * @param curve       curve name
     * @param reKey       re-encryption key
     * @param keyPairFrom key pair
     * @return EC parameters
     */
    public static ECParameterSpec getEcParameterSpec(
            String curve, WrapperReEncryptionKey reKey, KeyPair keyPairFrom) {
        ECParameterSpec ecSpec;
        if (curve != null) {
            ecSpec = ECNamedCurveTable.getParameterSpec(curve);
        } else {
            ecSpec = ((ECPrivateKey) keyPairFrom.getPrivate()).getParameters();
        }
        assertEquals(ecSpec, reKey.getECParameterSpec());
        return ecSpec;
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
        ECParameterSpec ecSpec = KeyUtilsTest.getEcParameterSpec(
                null, reKey, keyPairFrom);

        byte[] byteContent;
        switch (algorithm) {
            case BBS98:
                byteContent = new byte[WrapperBBS98.getMessageLength(ecSpec)];
                break;
            case ELGAMAL:
                byteContent = new byte[WrapperElGamalPRE.getMessageLength(ecSpec)];
                break;
            default:
                throw new CommonException();
        }

        random.nextBytes(byteContent);
        byte[] encrypted = KeyUtils.encryptDEK(
                algorithm,
                keyPairFrom.getPublic(),
                AESKeyGenerators.create(byteContent, Constants.SYMMETRIC_ALGORITHM),
                new SecureRandom());
        byte[] reEncrypted = KeyUtils.reEncryptEDEK(encrypted, reKey);
        byte[] decrypted = KeyUtils.decryptEDEK(algorithm, keyPairTo.getPrivate(),
                reEncrypted, isComplex);
        assertArrayEquals(byteContent, decrypted);
    }

}
