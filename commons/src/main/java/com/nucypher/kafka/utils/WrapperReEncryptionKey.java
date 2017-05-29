package com.nucypher.kafka.utils;

import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.util.encoders.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable wrapper for the re-encryption key and {@link ECParameterSpec}
 */
public class WrapperReEncryptionKey {

    private static final byte[] DELIMITER = "\n".getBytes();

    private BigInteger key;
    private ECParameterSpec ecSpec;
    private byte[] encryptedRandomKey;
    private EncryptionAlgorithm algorithm;

    /**
     * Empty key
     */
    public WrapperReEncryptionKey() {

    }

    /**
     * @param algorithm encryption algorithm
     * @param key       the re-encryption key
     * @param ecSpec    {@link ECParameterSpec} for the key
     */
    public WrapperReEncryptionKey(
            EncryptionAlgorithm algorithm, BigInteger key, ECParameterSpec ecSpec) {
        this.algorithm = algorithm;
        this.key = key;
        this.ecSpec = ecSpec;
    }

    /**
     * @param algorithm encryption algorithm
     * @param key       the re-encryption key data
     * @param ecSpec    {@link ECParameterSpec} for the key
     */
    public WrapperReEncryptionKey(
            EncryptionAlgorithm algorithm, byte[] key, ECParameterSpec ecSpec) {
        this(algorithm, new BigInteger(key), ecSpec);
    }

    /**
     * @param algorithm          encryption algorithm
     * @param key                the re-encryption key data
     * @param ecSpec             {@link ECParameterSpec} for the key
     * @param encryptedRandomKey encrypted random key
     */
    public WrapperReEncryptionKey(EncryptionAlgorithm algorithm,
                                  byte[] key,
                                  ECParameterSpec ecSpec,
                                  byte[] encryptedRandomKey) {
        this(algorithm, key, ecSpec);
        this.encryptedRandomKey = encryptedRandomKey;
    }

    /**
     * @param algorithm          encryption algorithm
     * @param key                the re-encryption key
     * @param ecSpec             {@link ECParameterSpec} for the key
     * @param encryptedRandomKey encrypted random key
     */
    public WrapperReEncryptionKey(EncryptionAlgorithm algorithm,
                                  BigInteger key,
                                  ECParameterSpec ecSpec,
                                  byte[] encryptedRandomKey) {
        this(algorithm, key, ecSpec);
        this.encryptedRandomKey = encryptedRandomKey;
    }

    /**
     * @return encryption algorithm
     */
    public EncryptionAlgorithm getAlgorithm() {
        return algorithm;
    }

    /**
     * @return the re-encryption key data
     */
    public BigInteger getReEncryptionKey() {
        return new BigInteger(key.toByteArray());
    }

    /**
     * @return {@link ECParameterSpec} for the key
     */
    public ECParameterSpec getECParameterSpec() {
        return ecSpec;
    }

    /**
     * @return encrypted random key
     */
    public byte[] getEncryptedRandomKey() {
        return encryptedRandomKey;
    }

    /**
     * @return is re-encryption key is empty or not
     */
    public boolean isEmpty() {
        return key == null;
    }

    /**
     * @return is re-encryption key is generated from private keys or not
     */
    public boolean isSimple() {
        return !isEmpty() && encryptedRandomKey == null;
    }

    /**
     * @return is re-encryption key is generated from private and public keys or not
     */
    public boolean isComplex() {
        return !isEmpty() && encryptedRandomKey != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrapperReEncryptionKey that = (WrapperReEncryptionKey) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(ecSpec, that.ecSpec) &&
                Arrays.equals(encryptedRandomKey, that.encryptedRandomKey) &&
                Objects.equals(algorithm != null ? algorithm.getClass() : null,
                        that.algorithm != null ? that.algorithm.getClass() : null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, ecSpec, encryptedRandomKey,
                algorithm != null ? algorithm.getClass() : null);
    }

    /**
     * @return byte array from the key and parameters
     */
    public byte[] toByteArray() {
        if (key == null) {
            return null;
        }
        ByteArrayOutputStream keyData = new ByteArrayOutputStream();
        try {
            keyData.write(algorithm.getClass().getCanonicalName().getBytes());
            keyData.write(DELIMITER);
            Base64.encode(key.toByteArray(), keyData);
            keyData.write(DELIMITER);
            Base64.encode(KeyUtils.ecSpecToByteArray(ecSpec), keyData);
            if (encryptedRandomKey != null) {
                keyData.write(DELIMITER);
                Base64.encode(encryptedRandomKey, keyData);
            }
        } catch (IOException e) {
            throw new CommonException(e);
        }
        return keyData.toByteArray();
    }

    /**
     * Get instance of {@link WrapperReEncryptionKey} from byte array
     *
     * @param data byte array with key data
     * @return {@link WrapperReEncryptionKey}
     * @throws IOException if there is a problem parsing the data
     */
    public static WrapperReEncryptionKey getInstance(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return new WrapperReEncryptionKey();
        }
        int[] indices = new int[4];
        int j = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == DELIMITER[0]) {
                indices[j] = i;
                j++;
            }
        }

        byte[] encryptedRandomKey = null;
        if (indices[2] != 0) {
            if (indices[3] == 0) {
                indices[3] = data.length;
            }
            encryptedRandomKey = Base64.decode(
                    Arrays.copyOfRange(data, indices[2] + DELIMITER.length, indices[3]));
        } else {
            indices[2] = data.length;
        }
        String algorithmClass = new String(Arrays.copyOf(data, indices[0]));
        EncryptionAlgorithm algorithm = EncryptionAlgorithmUtils
                .getEncryptionAlgorithmByClass(algorithmClass);
        byte[] keyData = Base64.decode(
                Arrays.copyOfRange(data, indices[0] + DELIMITER.length, indices[1]));
        byte[] ecSpecData = Base64.decode(
                Arrays.copyOfRange(data, indices[1] + DELIMITER.length, indices[2]));
        Object param = ASN1Primitive.fromByteArray(ecSpecData);
        X9ECParameters ecParameters = X9ECParameters.getInstance(param);
        ECParameterSpec ecSpec = KeyUtils.ecParametersToSpec(ecParameters);

        return new WrapperReEncryptionKey(algorithm, keyData, ecSpec, encryptedRandomKey);
    }
}
