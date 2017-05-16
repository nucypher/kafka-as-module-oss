package com.nucypher.kafka.utils;

import com.nucypher.kafka.Constants;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

/**
 * EC key generator which uses HMAC
 */
public class HmacKeyGenerator {

    private static final int POSITIVE_SIGNUM = 1;

    private Mac[] macGenerators;
    private KeyFactory keyFactory;
    private ECParameterSpec ecParameterSpec;
    private int bitLength;
    private int byteLength;

    /**
     * @param privateKey EC private key
     */
    public HmacKeyGenerator(PrivateKey privateKey) {
        ECPrivateKey ecPrivateKey = (ECPrivateKey) privateKey;
        ecParameterSpec = ecPrivateKey.getParameters();
        String algorithm = getAlgorithm(ecParameterSpec);
        bitLength = ecParameterSpec.getN().bitLength();
        byteLength = bitLength / 8 + (bitLength % 8 == 0 ? 0 : 1);
        try {
            Mac init = Mac.getInstance(algorithm, Constants.SECURITY_PROVIDER_NAME);
            int macCount = (int) Math.ceil((double) byteLength / init.getMacLength());
            macGenerators = new Mac[macCount];
            byte[] keyBytes = toByteArray(ecPrivateKey.getD());
            byte[][] keyChunks = split(keyBytes, macCount, init.getMacLength());
            for (int i = 0; i < macCount; i++) {
                macGenerators[i] = Mac.getInstance(init.getAlgorithm(), init.getProvider());
                macGenerators[i].init(new SecretKeySpec(keyChunks[i], algorithm));
            }
            keyFactory = KeyFactory.getInstance(
                    Constants.KEY_FACTORY_ALGORITHM, Constants.SECURITY_PROVIDER_NAME);
        } catch (NoSuchAlgorithmException |
                NoSuchProviderException |
                InvalidKeyException e) {
            throw new CommonException(e);
        }
    }

    private static String getAlgorithm(ECParameterSpec ecSpec) {
        int bitLength = ecSpec.getN().bitLength();
        if (bitLength > 384) {
            return "HmacSHA512";
        }
        if (bitLength > 256) {
            return "HmacSHA384";
        }
        if (bitLength > 224) {
            return "HmacSHA256";
        }
        return "HmacSHA224";
    }

    private static byte[][] split(byte[] bytes, int count, int size) {
        byte[][] result = new byte[count][];
        if (count == 1) {
            result[0] = bytes;
            return result;
        }
        for (int i = 0; i < count; i++) {
            result[i] = new byte[size];
        }

        int length = bytes.length;
        int counter = 0;
        for (int i = 0; i < length - size + 1; i += size) {
            result[counter++] = Arrays.copyOfRange(bytes, i, i + size);
        }
        if (length % size != 0) {
            System.arraycopy(bytes, length - length % size, result[counter], 0, length % size);
        }

        return result;
    }

    /**
     * Generate new EC private key from input message
     *
     * @param message input message
     * @return EC private key
     */
    public PrivateKey generatePrivateKey(String message) {
        return generatePrivateKey(message.getBytes());
    }

    /**
     * Generate new EC private key from input message
     *
     * @param message input message bytes
     * @return EC private key
     */
    public PrivateKey generatePrivateKey(byte[] message) {
        byte[] key = new byte[byteLength];
        int startIndex = 0;
        for (int i = 0; i < macGenerators.length - 1; i++) {
            Mac macGenerator = macGenerators[i];
            byte[] mac = macGenerator.doFinal(message);
            System.arraycopy(mac, 0, key, startIndex, macGenerator.getMacLength());
            startIndex += macGenerator.getMacLength();
        }
        int lastIndex = macGenerators.length - 1;
        Mac macGenerator = macGenerators[lastIndex];
        byte[] mac = macGenerator.doFinal(message);
        System.arraycopy(mac, 0, key, startIndex, key.length - startIndex);

        int excessBits = 8 * byteLength - bitLength;
        key[0] &= (1 << (8 - excessBits)) - 1;

        ECPrivateKeySpec privateKeySpec = new ECPrivateKeySpec(
                new BigInteger(POSITIVE_SIGNUM, key), ecParameterSpec);
        try {
            return keyFactory.generatePrivate(privateKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new CommonException(e);
        }
    }

    /**
     * Serialize {@link BigInteger} to byte array and cut sign byte if necessary
     *
     * @param value value for serializing
     * @return byte array
     */
    public static byte[] toByteArray(BigInteger value) { //TODO move to common utils
        byte[] valueBytes = value.toByteArray();
        if (value.bitLength() % 8 != 0) {
            return valueBytes;
        }

        return Arrays.copyOfRange(valueBytes, 1, valueBytes.length);
    }

}
