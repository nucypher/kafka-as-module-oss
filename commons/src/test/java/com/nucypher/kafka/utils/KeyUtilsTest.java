package com.nucypher.kafka.utils;

import com.nucypher.crypto.bbs98.WrapperBBS98;
import com.nucypher.crypto.elgamal.WrapperElGamalPRE;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Random;

import static com.nucypher.kafka.utils.KeyType.DEFAULT;
import static com.nucypher.kafka.utils.KeyType.PRIVATE;
import static com.nucypher.kafka.utils.KeyType.PRIVATE_AND_PUBLIC;
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

    /**
     * Test generating re-encryption key with wrong key type
     */
    @Test
    public void testGenerateReKeyWithWrongKeyType2() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("must contain private key"));
        String from = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        String to = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        testGenerateComplexReKey(algorithm, from, to, null, null, PRIVATE);
    }

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
        ECParameterSpec ecSpec = getECParameterSpec(curve, reKey, keyPairFrom);

        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        random.nextBytes(byteContent);
        byte[] encrypted;
        byte[] reEncrypted;
        byte[] decrypted;
        switch (algorithm) {
            case BBS98:
                WrapperBBS98 wrapperBBS98 = new WrapperBBS98(ecSpec, new SecureRandom());
                encrypted = wrapperBBS98.encrypt(keyPairFrom.getPublic(), byteContent);
                reEncrypted = wrapperBBS98.reencrypt(reKey.getReEncryptionKey(), encrypted);
                decrypted = wrapperBBS98.decrypt(keyPairTo.getPrivate(), reEncrypted);
                break;
            case ELGAMAL:
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

    /**
     * Test generating complex re-encryption key
     *
     * @param algorithm encryption algorithm
     * @param from      key from
     * @param to        key to
     * @param privateTo private key to
     * @param curve     curve name
     * @param keyType   key type
     */
    public static void testGenerateComplexReKey(
            EncryptionAlgorithm algorithm,
            String from,
            String to,
            String privateTo,
            String curve,
            KeyType keyType)
            throws Exception {
        WrapperReEncryptionKey reKey = KeyUtils.generateReEncryptionKey(
                algorithm, from, to, keyType, curve);

        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(privateTo);
        ECParameterSpec ecSpec = getECParameterSpec(curve, reKey, keyPairFrom);

        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        random.nextBytes(byteContent);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm, keyPairTo.getPrivate(), keyPairFrom.getPublic(), false);
        byte[] encrypted = keyManager.encryptDEK(
                AESKeyGenerators.create(byteContent, Constants.SYMMETRIC_ALGORITHM));
        byte[] reEncrypted = keyManager.reEncryptEDEK(encrypted, reKey);
        byte[] decrypted = keyManager.decryptEDEK(reEncrypted, true).getEncoded();
        assertArrayEquals(byteContent, decrypted);
    }

    /**
     * Get EC parameters
     *
     * @param curve       curve name
     * @param reKey       re-encryption key
     * @param keyPairFrom key pair
     * @return EC parameters
     */
    public static ECParameterSpec getECParameterSpec(
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

    /**
     * Test generating public key from private key
     */
    @Test
    public void testGeneratePublicKey() throws Exception {
        String filename = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        testGeneratePublicKey(filename);
        filename = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        testGeneratePublicKey(filename);
    }

    private static void testGeneratePublicKey(String filename) throws Exception {
        ECPrivateKey privateKey = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filename).getPrivate();
        ECParameterSpec ecSpec = privateKey.getParameters();
        PublicKey publicKey = KeyUtils.generatePublicKey(privateKey);

        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                TestConstants.ENCRYPTION_ALGORITHM, privateKey, publicKey, false);
        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        random.nextBytes(byteContent);
        byte[] encrypted = keyManager.encryptDEK(
                AESKeyGenerators.create(byteContent, Constants.SYMMETRIC_ALGORITHM));
        byte[] decrypted = keyManager.decryptEDEK(encrypted, false).getEncoded();
        assertArrayEquals(byteContent, decrypted);
    }
}
