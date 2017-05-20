package com.nucypher.kafka.utils;

import com.nucypher.kafka.errors.CommonException;
import org.hamcrest.core.StringContains;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.security.PrivateKey;

import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link GranularUtils}
 *
 * @author szotov
 */
public class GranularUtilsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test generating private keys from messages
     */
    @Test
    public void testGeneratePrivateKeysFromMessages() throws Exception {
        String privateKey = getClass().getResource("/private-key-prime256v1-1.pem").getPath();
        PrivateKey key = GranularUtils.deriveKeyFromData(privateKey, "a.c");
        assertNotNull(key);
    }

    /**
     * Test generating private keys from messages with public key
     */
    @Test
    public void testGeneratePrivateKeysFromMessagesWithException() throws Exception {
        expectedException.expect(CommonException.class);
        expectedException.expectMessage(StringContains.containsString("must contain private key"));
        String key = getClass().getResource("/public-key-secp521r1-2.pem").getPath();
        GranularUtils.deriveKeyFromData(key, null);
    }

}
