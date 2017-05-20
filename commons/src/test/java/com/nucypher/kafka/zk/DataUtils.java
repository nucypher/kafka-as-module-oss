package com.nucypher.kafka.zk;

import com.nucypher.kafka.TestConstants;
import com.nucypher.kafka.clients.granular.StructuredDataAccessorStub;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.spec.ECParameterSpec;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Class with data generators for tests
 *
 * @author szotov
 */
public class DataUtils {

    private static final EncryptionAlgorithm ALGORITHM = TestConstants.ENCRYPTION_ALGORITHM;
    private static final Random RANDOM = new Random();
    private static final ECParameterSpec EC_SPEC =
            ECNamedCurveTable.getParameterSpec("secp521r1");

    /**
     * @return empty re-encryption key
     */
    public static WrapperReEncryptionKey getReEncryptionKeyEmpty() {
        return new WrapperReEncryptionKey();
    }

    /**
     * @return random re-encryption key
     */
    public static WrapperReEncryptionKey getReEncryptionKeySimple() {
        byte[] data1 = new byte[7];
        RANDOM.nextBytes(data1);
        return new WrapperReEncryptionKey(ALGORITHM, data1, EC_SPEC);
    }

    /**
     * @return random re-encryption key with additional key
     */
    public static WrapperReEncryptionKey getReEncryptionKeyComplex() {
        byte[] data1 = new byte[7];
        RANDOM.nextBytes(data1);
        byte[] data2 = new byte[14];
        RANDOM.nextBytes(data2);
        return new WrapperReEncryptionKey(ALGORITHM, data1, EC_SPEC, data2, 111);
    }

    /**
     * @return random date after current date
     */
    public static Long getExpiredMillis() {
        return System.currentTimeMillis() + TimeUnit.DAYS.toMillis(RANDOM.nextInt());
    }

    /**
     * Convert expired date to byte array
     *
     * @param expired expired date
     * @return converted date
     */
    public static byte[] getByteArrayFromExpired(Long expired) {
        return BigInteger.valueOf(expired).toByteArray();
    }

    /**
     * @return bytes for {@link EncryptionType#FULL}
     */
    public static byte[] getFullEncryptedChannel() {
        return new byte[]{EncryptionType.FULL.getCode()};
    }

    /**
     * @return bytes for {@link EncryptionType#GRANULAR} and {@link StructuredDataAccessorStub}
     */
    public static byte[] getPartialEncryptedChannel() {
        byte[] classNameBytes = StructuredDataAccessorStub.class.getCanonicalName().getBytes();
        byte[] data = new byte[classNameBytes.length + 1];
        data[0] = EncryptionType.GRANULAR.getCode();
        System.arraycopy(classNameBytes, 0, data, 1, classNameBytes.length);
        return data;
    }

}
