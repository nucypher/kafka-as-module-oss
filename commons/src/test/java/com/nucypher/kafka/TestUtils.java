package com.nucypher.kafka;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.crypto.impl.ElGamalEncryptionAlgorithm;
import com.nucypher.kafka.utils.EncryptionAlgorithmUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * Utils for tests
 */
public class TestUtils {

    /**
     * Test file which contains EC key pair
     */
    public static final String PEM = "P521.pem";

    /**
     * Encryption algorithm for tests
     */
    public static final EncryptionAlgorithm ENCRYPTION_ALGORITHM =
            new ElGamalEncryptionAlgorithm();

    /**
     * Class of encryption algorithm for tests
     */
    public static final Class<? extends EncryptionAlgorithm> ENCRYPTION_ALGORITHM_CLASS =
            ENCRYPTION_ALGORITHM.getClass();

    /**
     * @return available encryption algorithms
     */
    public static Collection<Object[]> getEncryptionAlgorithms() {
        Set<String> algorithms = EncryptionAlgorithmUtils.getAvailableAlgorithms();
        Object[][] data = new Object[algorithms.size()][];
        int i = 0;
        for (String algorithm : algorithms) {
            data[i] = new Object[]{
                    EncryptionAlgorithmUtils.getEncryptionAlgorithm(algorithm)};
            i++;
        }
//        Object[][] data = new Object[][]{
//                {new BBS98EncryptionAlgorithm()},
//                {new ElGamalEncryptionAlgorithm()}
//        };
        return Arrays.asList(data);
    }

}
