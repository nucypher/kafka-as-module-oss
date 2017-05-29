package com.nucypher.kafka.utils;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.TestUtils;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Parameterized test for {@link KeyUtils} using encryption algorithms
 *
 * @author szotov
 */
@RunWith(value = Parameterized.class)
public final class KeyUtilsAlgorithmTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

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
        return TestUtils.getEncryptionAlgorithms();
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

        assertEquals(algorithm.getClass(), wrapper.getAlgorithm().getClass());
        assertArrayEquals(key, wrapper.getReEncryptionKey().toByteArray());
        assertEquals(ecSpec, wrapper.getECParameterSpec());

        byte[] encryptedRandomKey = new byte[32];
        random.nextBytes(encryptedRandomKey);

        wrapper = new WrapperReEncryptionKey(algorithm, key, ecSpec, encryptedRandomKey);
        serializedData = wrapper.toByteArray();
        wrapper = WrapperReEncryptionKey.getInstance(serializedData);

        assertEquals(algorithm.getClass(), wrapper.getAlgorithm().getClass());
        assertArrayEquals(key, wrapper.getReEncryptionKey().toByteArray());
        assertEquals(ecSpec, wrapper.getECParameterSpec());
        assertArrayEquals(encryptedRandomKey, wrapper.getEncryptedRandomKey());
    }

}
