package com.nucypher.kafka.encrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static com.nucypher.kafka.utils.KeyType.DEFAULT;
import static com.nucypher.kafka.utils.KeyType.PRIVATE;
import static com.nucypher.kafka.utils.KeyType.PRIVATE_AND_PUBLIC;
import static com.nucypher.kafka.utils.KeyType.PUBLIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link ReEncryptionKeyManager}
 *
 * @author szotov
 */
@RunWith(value = Parameterized.class)
public final class ReEncryptionKeyManagerTest {

    private static final Random RANDOM = new Random();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private EncryptionAlgorithm algorithm;
    private ReEncryptionKeyManager reEncryptionKeyManager;

    /**
     * @param algorithm {@link EncryptionAlgorithm}
     */
    public ReEncryptionKeyManagerTest(EncryptionAlgorithm algorithm) {
        this.algorithm = algorithm;
        reEncryptionKeyManager = new ReEncryptionKeyManager(algorithm);
    }

    /**
     * @return collection of {@link EncryptionAlgorithm} values
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return TestUtils.getEncryptionAlgorithms();
    }

    /**
     * Test generating simple re-encryption key
     */
    @Test
    public void testGenerateSimpleReKey() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        testGenerateSimpleReKey(privateFrom, privateTo, "prime256v1");

        privateFrom = privateTo;
        testGenerateSimpleReKey(privateFrom, privateTo, "prime256v1");

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        testGenerateSimpleReKey(privateFrom, privateTo, "secp521r1");

        privateFrom = privateTo;
        testGenerateSimpleReKey(privateFrom, privateTo, "secp521r1");
    }

    /**
     * Test generating EC key pair
     */
    @Test
    public void testGenerateKey() throws IOException {
        String filePath = String.format("%s/%s.pem",
                testFolder.getRoot().getAbsolutePath(),
                UUID.randomUUID().toString());
        reEncryptionKeyManager.generateECKeyPairToPEM(filePath, "secp521r1", DEFAULT);
        assertTrue(new File(filePath).exists());
        ECPrivateKey key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filePath).getPrivate();
        assertNotNull(key.getParameters());
        assertEquals(ECNamedCurveTable.getParameterSpec("secp521r1"), key.getParameters());

        filePath = String.format("%s/%s.pem",
                testFolder.getRoot().getAbsolutePath(),
                UUID.randomUUID().toString());
        reEncryptionKeyManager.generateECKeyPairToPEM(filePath, "prime256v1", DEFAULT);
        assertTrue(new File(filePath).exists());
        key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filePath).getPrivate();
        assertNotNull(key.getParameters());
        assertEquals(ECNamedCurveTable.getParameterSpec("prime256v1"), key.getParameters());
    }

    /**
     * Test generating complex re-encryption key
     */
    @Test
    public void testGenerateComplexReKey() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        String publicTo = getClass().getResource("/public-key-prime256v1-2.pem").getPath();
        testGenerateComplexReKey(privateFrom, publicTo, privateTo, "prime256v1");

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        publicTo = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        testGenerateComplexReKey(privateFrom, publicTo, privateTo, "secp521r1");
    }

    private void testGenerateComplexReKey(
            String from,
            String publicTo,
            String privateTo,
            String curve)
            throws Exception {
        testGenerateComplexReKey(from, publicTo, privateTo, null, DEFAULT);
        testGenerateComplexReKey(from, publicTo, privateTo, curve, DEFAULT);
        testGenerateComplexReKey(from, privateTo, privateTo, null, PUBLIC);
        testGenerateComplexReKey(from, publicTo, privateTo, null, PRIVATE_AND_PUBLIC);
        testGenerateComplexReKey(from, publicTo, privateTo, null, PUBLIC);
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
        testGenerateSimpleReKey(from, to, null);
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
        testGenerateSimpleReKey(from, to, "secp521r1");
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
        testGenerateSimpleReKey(from, to, "secp521r1");
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
        testGenerateSimpleReKey(from, to, null);
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
        testGenerateComplexReKey(from, to, null, null, PRIVATE);
    }

    /**
     * Test generating simple re-encryption key
     *
     * @param from  key from
     * @param to    key to
     * @param curve curve name
     */
    private void testGenerateSimpleReKey(String from, String to, String curve) throws Exception {
        WrapperReEncryptionKey reKey =
                reEncryptionKeyManager.generateReEncryptionKey(from, to, DEFAULT, null);
        WrapperReEncryptionKey reKey1 =
                reEncryptionKeyManager.generateReEncryptionKey(from, to, DEFAULT, curve);
        WrapperReEncryptionKey reKey2 =
                reEncryptionKeyManager.generateReEncryptionKey(from, to, PRIVATE, null);
        WrapperReEncryptionKey reKey3 =
                reEncryptionKeyManager.generateReEncryptionKey(from, to, PRIVATE_AND_PUBLIC, null);
        assertEquals(reKey, reKey1);
        assertEquals(reKey, reKey2);
        assertEquals(reKey, reKey3);

        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(to);
        ECParameterSpec ecSpec = getECParameterSpec(curve, reKey, keyPairFrom);

        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        RANDOM.nextBytes(byteContent);
        byte[] encrypted = algorithm.encrypt(keyPairFrom.getPublic(), byteContent, new SecureRandom());
        byte[] reEncrypted = algorithm.reEncrypt(reKey.getReEncryptionKey(), ecSpec, encrypted);
        byte[] decrypted = algorithm.decrypt(keyPairTo.getPrivate(), reEncrypted);
        assertArrayEquals(byteContent, decrypted);
    }

    /**
     * Test generating complex re-encryption key
     *
     * @param from      key from
     * @param to        key to
     * @param privateTo private key to
     * @param curve     curve name
     * @param keyType   key type
     */
    private void testGenerateComplexReKey(
            String from,
            String to,
            String privateTo,
            String curve,
            KeyType keyType)
            throws Exception {
        WrapperReEncryptionKey reKey = reEncryptionKeyManager.generateReEncryptionKey(
                from, to, keyType, curve);

        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(privateTo);
        ECParameterSpec ecSpec = getECParameterSpec(curve, reKey, keyPairFrom);

        DataEncryptionKeyManager keyManager =
                new DataEncryptionKeyManager(algorithm, keyPairTo.getPrivate());
        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        RANDOM.nextBytes(byteContent);
        byte[] encrypted = algorithm.encrypt(keyPairFrom.getPublic(), byteContent, new SecureRandom());
        byte[] reEncrypted = keyManager.reEncryptEDEK("data", encrypted, reKey);
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

}
