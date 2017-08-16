package com.nucypher.kafka.encrypt;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.TestUtils;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.SubkeyGenerator;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Key;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Collection;

import static com.nucypher.kafka.utils.KeyType.PRIVATE;
import static com.nucypher.kafka.utils.KeyType.PUBLIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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
        return TestUtils.getEncryptionAlgorithms();
    }

    /**
     * Test re-encrypting EDEK
     *
     * @param algorithm encryption algorithm
     * @param from      key from
     * @param to        key to
     * @param isComplex is complex re-encryption
     */
    private static void testReEncryptEDEK(EncryptionAlgorithm algorithm,
                                          String from,
                                          String to,
                                          boolean isComplex) throws Exception {
        ReEncryptionKeyManager reEncryptionKeyManager = new ReEncryptionKeyManager(algorithm);
        WrapperReEncryptionKey reKey = reEncryptionKeyManager.generateReEncryptionKey(
                from, to, !isComplex ? PRIVATE : PUBLIC, null);
        KeyPair keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
        KeyPair keyPairTo = KeyUtils.getECKeyPairFromPEM(to);

        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm,
                keyPairTo.getPrivate(),
                keyPairFrom.getPublic(),
                false,
                null);
        Key key = keyManager.getDEK("");

        byte[] encrypted = keyManager.encryptDEK(key);
        byte[] reEncrypted = keyManager.reEncryptEDEK("data", encrypted, reKey);
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

    /**
     * Test complex re-encrypting EDEK
     */
    @Test
    public void testComplexReEncryptEDEK() throws Exception {
        String privateFrom = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String privateTo = getClass().getResource("/private-key-prime256v1-2.pem").getPath();
        testReEncryptEDEK(algorithm, privateFrom, privateTo, true);

        privateFrom = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        privateTo = getClass().getResource("/private-key-secp521r1-2.pem").getPath();
        testReEncryptEDEK(algorithm, privateFrom, privateTo, true);
    }

    /**
     * Test encrypting using derived key
     */
    @Test
    public void testEncryptEDEKUsingDerivedKey() throws Exception {
        String privateKey = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        testEncryptEDEKUsingDerivedKey(algorithm, privateKey);
        privateKey = getClass().getResource("/private-key-secp521r1-1.pem").getPath();
        testEncryptEDEKUsingDerivedKey(algorithm, privateKey);
    }

    private static void testEncryptEDEKUsingDerivedKey(EncryptionAlgorithm algorithm,
                                                       String privateKeyPath) throws Exception {
        String data = "data";
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(privateKeyPath);
        PrivateKey privateKey = SubkeyGenerator.deriveKey(keyPair.getPrivate(), data);

        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                algorithm,
                keyPair.getPrivate(),
                keyPair.getPublic(),
                true,
                null);
        Key key = keyManager.getDEK(data);
        byte[] encrypted = keyManager.encryptDEK(key, data);

        keyManager = new DataEncryptionKeyManager(algorithm, privateKey);
        Key decrypted = keyManager.decryptEDEK(encrypted, false);
        assertEquals(key, decrypted);
    }

    /**
     * Test changing DEK after exceeding max used
     */
    @Test
    public void testChangingDEK() throws Exception {
        String privateKey = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        String data = "data";
        KeyPair keyPair = KeyUtils.getECKeyPairFromPEM(privateKey);
        DataEncryptionKeyManager keyManager = new DataEncryptionKeyManager(
                null, null, keyPair.getPublic(), false, 2);
        Key key1 = keyManager.getDEK(data);
        Key key2 = keyManager.getDEK(data);
        assertEquals(key1, key2);
        Key key3 = keyManager.getDEK(data);
        assertNotEquals(key1, key3);

        keyManager = new DataEncryptionKeyManager(
                null, null, keyPair.getPublic(), false, null);
        key1 = keyManager.getDEK(data);
        key2 = keyManager.getDEK(data);
        assertEquals(key1, key2);
        key3 = keyManager.getDEK(data);
        assertEquals(key1, key3);
    }
}
