package com.nucypher.kafka.utils;

import com.nucypher.kafka.DefaultProvider;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import static com.nucypher.kafka.utils.KeyType.DEFAULT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Parameterized test for {@link KeyUtils} using encryption algorithm
 *
 * @author szotov
 */
@RunWith(value = Parameterized.class)
public final class KeyUtilsAlgorithmTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Random random = new Random();
    private EncryptionAlgorithm algorithm;

    /**
     * @param algorithm {@link EncryptionAlgorithm}
     */
    public KeyUtilsAlgorithmTest(EncryptionAlgorithm algorithm) {
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

    /**
     * Provider initialization
     */
    @BeforeClass
    public static void initialize() {
        DefaultProvider.initializeProvider();
    }

    /**
     * Test generating simple re-encryption key
     */
    @Test
    public void testGenerateSimpleReKey() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        KeyUtilsTest.testGenerateSimpleReKey(algorithm, privateFrom, privateTo, "prime256v1");

        privateFrom = privateTo;
        KeyUtilsTest.testGenerateSimpleReKey(algorithm, privateFrom, privateTo, "prime256v1");

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        KeyUtilsTest.testGenerateSimpleReKey(algorithm, privateFrom, privateTo, "secp521r1");

        privateFrom = privateTo;
        KeyUtilsTest.testGenerateSimpleReKey(algorithm, privateFrom, privateTo, "secp521r1");
    }

    /**
     * Test generating EC key pair
     */
    @Test
    public void testGenerateKey() throws IOException {
        String filePath = String.format("%s/%s.pem",
                testFolder.getRoot().getAbsolutePath(),
                UUID.randomUUID().toString());
        KeyUtils.generateECKeyPairToPEM(algorithm, filePath, "secp521r1", DEFAULT);
        assertTrue(new File(filePath).exists());
        ECPrivateKey key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filePath).getPrivate();
        assertNotNull(key.getParameters());
        assertEquals(ECNamedCurveTable.getParameterSpec("secp521r1"), key.getParameters());

        filePath = String.format("%s/%s.pem",
                testFolder.getRoot().getAbsolutePath(),
                UUID.randomUUID().toString());
        KeyUtils.generateECKeyPairToPEM(algorithm, filePath, "prime256v1", DEFAULT);
        assertTrue(new File(filePath).exists());
        key = (ECPrivateKey) KeyUtils.getECKeyPairFromPEM(filePath).getPrivate();
        assertNotNull(key.getParameters());
        assertEquals(ECNamedCurveTable.getParameterSpec("prime256v1"), key.getParameters());
    }

    /**
     * Test converting byte array to {@link WrapperReEncryptionKey} and back
     */
    @Test
    public void testWrapperReKey() throws IOException {
        ECParameterSpec ecSpec = ECNamedCurveTable.getParameterSpec("secp521r1");
        byte[] key = new byte[16];
        random.nextBytes(key);

        WrapperReEncryptionKey wrapper = new WrapperReEncryptionKey(algorithm, key, ecSpec);
        byte[] serializedData = wrapper.toByteArray();
        wrapper = WrapperReEncryptionKey.getInstance(serializedData);

        assertEquals(algorithm, wrapper.getAlgorithm());
        assertArrayEquals(key, wrapper.getReEncryptionKey().toByteArray());
        assertEquals(ecSpec, wrapper.getECParameterSpec());

        byte[] encryptedRandomKey = new byte[32];
        random.nextBytes(encryptedRandomKey);
        int randomKeyLength = random.nextInt();

        wrapper = new WrapperReEncryptionKey(
                algorithm, key, ecSpec, encryptedRandomKey, randomKeyLength);
        serializedData = wrapper.toByteArray();
        wrapper = WrapperReEncryptionKey.getInstance(serializedData);

        assertEquals(algorithm, wrapper.getAlgorithm());
        assertArrayEquals(key, wrapper.getReEncryptionKey().toByteArray());
        assertEquals(ecSpec, wrapper.getECParameterSpec());
        assertArrayEquals(encryptedRandomKey, wrapper.getEncryptedRandomKey());
        assertEquals(Integer.valueOf(randomKeyLength), wrapper.getRandomKeyLength());
    }

    /**
     * Test simple re-encrypting EDEK
     */
    @Test
    public void testSimpleReEncryptEDEK() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        KeyUtilsTest.testReEncryptEDEK(algorithm, privateFrom, privateTo, false);

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        KeyUtilsTest.testReEncryptEDEK(algorithm, privateFrom, privateTo, false);
    }

}
