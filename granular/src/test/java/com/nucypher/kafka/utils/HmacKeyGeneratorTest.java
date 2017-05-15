package com.nucypher.kafka.utils;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.TestConstants;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for {@link HmacKeyGenerator}
 *
 * @author szotov
 */
@RunWith(Parameterized.class)
public final class HmacKeyGeneratorTest {

    private static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM;

    private String curveName;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Provider initialization
     */
    @BeforeClass
    public static void initialize() {
        DefaultProvider.initializeProvider();
    }

    /**
     * @return array of available EC curve names
     */
    @Parameterized.Parameters(name = "{index}: Curve name {0}")
    public static Object[] data() {
        Set<Object> set = new HashSet<>();
        set.addAll(Collections.list(org.bouncycastle.asn1.x9.ECNamedCurveTable.getNames()));
        return set.toArray();
    }

    public HmacKeyGeneratorTest(String curveName) {
        this.curveName = curveName;
    }

    /**
     * Test generating key using HMAC
     */
    @Test
    public void testGenerateKey() throws IOException {
        for (int i = 0; i < 20; i++) {
            PrivateKey basePrivateKey = KeyUtils.generateECKeyPair(ALGORITHM, curveName)
                    .getFirst().getPrivate();

            String message = "a.b.2";
            HmacKeyGenerator hmacGenerator = new HmacKeyGenerator(basePrivateKey);
            PrivateKey generatedPrivateKey1 = hmacGenerator.generatePrivateKey(message);
            // ECPrivateKey generatedPrivateKey1 = (ECPrivateKey)
            // hmacGenerator.generatePrivateKey(message);
            // ECPrivateKey ecBasePrivateKey = (ECPrivateKey) basePrivateKey;
            // assertTrue(HmacKeyGenerator.toByteArray(generatedPrivateKey1.getD()).length
            // >=
            // HmacKeyGenerator.toByteArray(ecBasePrivateKey.getD()).length);

            message = "a.b";
            PrivateKey generatedPrivateKey2 = hmacGenerator.generatePrivateKey(message);
            assertNotEquals(generatedPrivateKey1, generatedPrivateKey2);
            hmacGenerator = new HmacKeyGenerator(basePrivateKey);
            PrivateKey generatedPrivateKey3 = hmacGenerator.generatePrivateKey(message);
            assertEquals(generatedPrivateKey2, generatedPrivateKey3);

            basePrivateKey = KeyUtils.generateECKeyPair(ALGORITHM, curveName)
                    .getFirst().getPrivate();
            hmacGenerator = new HmacKeyGenerator(basePrivateKey);
            PrivateKey generatedPrivateKey4 = hmacGenerator.generatePrivateKey(message);
            assertNotEquals(generatedPrivateKey3, generatedPrivateKey4);
        }
    }

    @Test
    public void testDeterministic() throws IOException {
        PrivateKey basePrivateKey = KeyUtils.generateECKeyPair(ALGORITHM, curveName)
                .getFirst().getPrivate();

        String message = "a.b.2";
        HmacKeyGenerator hmacGenerator = new HmacKeyGenerator(basePrivateKey);
        PrivateKey generatedPrivateKey1 = hmacGenerator.generatePrivateKey(message);

        hmacGenerator = new HmacKeyGenerator(basePrivateKey);
        PrivateKey generatedPrivateKey2 = hmacGenerator.generatePrivateKey(message);

        assertEquals(generatedPrivateKey1, generatedPrivateKey2);

    }

    @Test
    public void testStateless() throws IOException {
        PrivateKey basePrivateKey = KeyUtils.generateECKeyPair(ALGORITHM, curveName)
                .getFirst().getPrivate();

        String message = "a.b.2";
        HmacKeyGenerator hmacGenerator = new HmacKeyGenerator(basePrivateKey);
        PrivateKey generatedPrivateKey1 = hmacGenerator.generatePrivateKey(message);

        hmacGenerator = new HmacKeyGenerator(basePrivateKey);
        PrivateKey generatedPrivateKey2 = hmacGenerator.generatePrivateKey(message);

        assertEquals(generatedPrivateKey1, generatedPrivateKey2);

    }
}
