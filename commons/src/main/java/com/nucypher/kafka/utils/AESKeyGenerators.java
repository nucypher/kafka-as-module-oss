package com.nucypher.kafka.utils;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;

import javax.crypto.KeyGenerator;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;
import java.util.Map;

import static com.nucypher.kafka.Constants.BOUNCY_CASTLE_PROVIDER_NAME;
import static com.nucypher.kafka.Constants.AES_ALGORITHM_NAME;

/**
 * AES key generators
 */
public class AESKeyGenerators {

    private static final int BITS_IN_BYTE = 8;
    private static final Map<Integer, KeyGenerator> keyGenerators = new HashMap<>();

    static {
        DefaultProvider.initializeProvider();
        getKeyGenerator(128);
        getKeyGenerator(192);
        getKeyGenerator(256);
    }

    private static KeyGenerator getKeyGenerator(int size) {
        KeyGenerator keyGenerator = keyGenerators.get(size);
        if (keyGenerator == null) {
            synchronized (AESKeyGenerators.class) {
                keyGenerator = keyGenerators.get(size);
                if (keyGenerator == null) {
                    try {
                        keyGenerator = KeyGenerator.getInstance(
                                AES_ALGORITHM_NAME, BOUNCY_CASTLE_PROVIDER_NAME);
                    } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
                        throw new CommonException(e);
                    }
                    keyGenerator.init(size);
                    keyGenerators.put(size, keyGenerator);
                }
            }
        }
        return keyGenerator;
    }

    /**
     * Generate DEK - AES
     *
     * @param size key size in bytes
     * @return DEK
     */
    public static Key generateDEK(int size) {
        size = BITS_IN_BYTE * size;
        KeyGenerator keyGenerator = getKeyGenerator(size);
        return keyGenerator.generateKey();
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
