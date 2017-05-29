package com.nucypher.kafka.utils;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.TestUtils;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.junit.Test;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Random;

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

    private static final EncryptionAlgorithm ALGORITHM = TestUtils.ENCRYPTION_ALGORITHM;
    private static final Random RANDOM = new Random();

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
        byte[] byteContent = new byte[KeyUtils.getMessageLength(ecSpec)];
        RANDOM.nextBytes(byteContent);
        byte[] encrypted = ALGORITHM.encrypt(publicKey, byteContent, new SecureRandom());
        byte[] decrypted = ALGORITHM.decrypt(privateKey, encrypted);
        assertArrayEquals(byteContent, decrypted);
    }
}
