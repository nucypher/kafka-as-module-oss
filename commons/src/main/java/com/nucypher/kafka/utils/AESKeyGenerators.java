package com.nucypher.kafka.utils;

import com.nucypher.kafka.errors.CommonException;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

import static com.nucypher.kafka.Constants.SECURITY_PROVIDER_NAME;
import static com.nucypher.kafka.Constants.SYMMETRIC_ALGORITHM;

/**
 * AES key generators
 */
public class AESKeyGenerators {
    /**
     * AES 128 key generator
     */
    public static final KeyGenerator AES_128;
    /**
     * AES 192 key generator
     */
    public static final KeyGenerator AES_192;
    /**
     * AES 256 key generator
     */
    public static final KeyGenerator AES_256;

    static {
        try {
            AES_128 = KeyGenerator.getInstance(SYMMETRIC_ALGORITHM, SECURITY_PROVIDER_NAME);
            AES_128.init(128);

            AES_192 = KeyGenerator.getInstance(SYMMETRIC_ALGORITHM, SECURITY_PROVIDER_NAME);
            AES_192.init(192);

            AES_256 = KeyGenerator.getInstance(SYMMETRIC_ALGORITHM, SECURITY_PROVIDER_NAME);
            AES_256.init(256);
        } catch (Exception ex) {
            throw new CommonException("Unable to initialize symmetric key generators", ex);
        }
    }

    /**
     * Generate DEK - AES 256 right now only
     *
     * @return - Key
     */
    public static Key generateDEK() {
        // TODO parameterize via symmetric algorithm and key size
        return AESKeyGenerators.AES_256.generateKey();
    }

    /**
     * Create Key from byte array
     *
     * @param keyBytes               - byte[]
     * @param symmetricAlgorithmName - "AES"
     * @return Key
     */
    public static Key create(byte[] keyBytes, String symmetricAlgorithmName) {
        return new SecretKeySpec(keyBytes, 0, keyBytes.length, symmetricAlgorithmName);
    }

}
