package com.nucypher.kafka.encrypt;

import com.google.common.primitives.Ints;
import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.KeyType;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECKey;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.SecureRandom;

/**
 * Class for generating EC keys
 */
public class ReEncryptionKeyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReEncryptionKeyManager.class);

    static {
        DefaultProvider.initializeProvider();
    }

    private EncryptionAlgorithm algorithm;

    /**
     * Holder for {@link KeyPair} and {@link ECParameterSpec}
     */
    public static class KeyPairHolder {
        private KeyPair keyPair;
        private ECParameterSpec ecParameterSpec;

        /**
         * @param keyPair         {@link KeyPair}
         * @param ecParameterSpec {@link ECParameterSpec}
         */
        public KeyPairHolder(KeyPair keyPair, ECParameterSpec ecParameterSpec) {
            this.keyPair = keyPair;
            this.ecParameterSpec = ecParameterSpec;
        }

        /**
         * @return {@link KeyPair}
         */
        public KeyPair getKeyPair() {
            return keyPair;
        }

        /**
         * @return {@link ECParameterSpec}
         */
        public ECParameterSpec getEcParameterSpec() {
            return ecParameterSpec;
        }
    }

    /**
     * @param algorithm encryption algorithm
     */
    public ReEncryptionKeyManager(EncryptionAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Generate re-encryption key from two EC private keys
     *
     * @param from      full path to the first key file
     * @param to        full path to the second key file
     * @param keyTypeTo key type of the second file
     * @param curve     the name of the curve requested. If null then used EC
     *                  parameters from keys
     * @return re-encryption key
     * @throws CommonException if files does not contain right keys or parameters
     */
    public WrapperReEncryptionKey generateReEncryptionKey(
            String from,
            String to,
            KeyType keyTypeTo,
            String curve) throws CommonException {
        KeyPair keyPairFrom;
        KeyPair keyPairTo;
        try {
            keyPairFrom = KeyUtils.getECKeyPairFromPEM(from);
            keyPairTo = KeyUtils.getECKeyPairFromPEM(to);
        } catch (IOException e) {
            throw new CommonException(e);
        }

        WrapperReEncryptionKey result =
                generateReEncryptionKey(keyPairFrom, keyPairTo, keyTypeTo, curve);
        LOGGER.debug("Re-encryption key '{}, {}' was generated", from, to);
        return result;
    }

    /**
     * Generate re-encryption key from two EC private keys
     *
     * @param keyPairFrom the first key pair
     * @param keyPairTo   the second key pair
     * @param keyTypeTo   key type of the second key pair
     * @param curve       the name of the curve requested. If null then used EC
     *                    parameters from keys
     * @return re-encryption key
     * @throws CommonException if files does not contain right keys or parameters
     */
    public WrapperReEncryptionKey generateReEncryptionKey(
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            KeyType keyTypeTo,
            String curve) throws CommonException {
        if (keyPairFrom.getPrivate() == null) {
            throw new CommonException("First key pair must contain private key");
        }
        ECParameterSpec ecSpec = getECParameterSpec(curve, keyPairFrom, keyPairTo);
        return generateReEncryptionKey(keyPairFrom, keyPairTo, keyTypeTo, ecSpec);
    }

    private WrapperReEncryptionKey generateReEncryptionKey(
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            KeyType keyTypeTo,
            ECParameterSpec ecSpec) {
        WrapperReEncryptionKey result;
        switch (keyTypeTo) {
            case DEFAULT:
            case PRIVATE_AND_PUBLIC:
                if (keyPairTo.getPrivate() != null) {
                    result = getSimpleReEncryptionKey(keyPairFrom, keyPairTo, ecSpec);
                } else {
                    result = getComplexReEncryptionKey(keyPairFrom, keyPairTo, ecSpec);
                }
                break;
            case PRIVATE:
                if (keyPairTo.getPrivate() == null) {
                    throw new CommonException(
                            "Second key pair with type '%s' must contain private key", keyTypeTo);
                }
                result = getSimpleReEncryptionKey(keyPairFrom, keyPairTo, ecSpec);
                break;
            case PUBLIC:
                if (keyPairTo.getPublic() == null) {
                    throw new CommonException(
                            "Second key pair with type '%s' must contain public key", keyTypeTo);
                }
                result = getComplexReEncryptionKey(keyPairFrom, keyPairTo, ecSpec);
                break;
            default:
                throw new CommonException("Unsupported key type '%s'", keyTypeTo);
        }

        LOGGER.debug("Re-encryption key was generated");
        return result;
    }

    private WrapperReEncryptionKey getSimpleReEncryptionKey(
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            ECParameterSpec ecSpec) {
        LOGGER.debug("Used private keys for generating re-encryption key");
        BigInteger reEncryptionKey = algorithm.generateReEncryptionKey(
                keyPairFrom.getPrivate(), keyPairTo.getPrivate());
        return new WrapperReEncryptionKey(algorithm, reEncryptionKey, ecSpec);
    }

    private WrapperReEncryptionKey getComplexReEncryptionKey(
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            ECParameterSpec ecSpec) {
        LOGGER.debug("Used private and public keys for generating re-encryption key");
        //generating random EC private key
        KeyPair keyPair = generateECKeyPair(ecSpec).getKeyPair();
        ECPrivateKey ecPrivateKeyRandom = (ECPrivateKey) keyPair.getPrivate();
        //generating 'private from -> random private' re-encryption key
        WrapperReEncryptionKey reEncryptionKey = generateReEncryptionKey(
                keyPairFrom, keyPair, KeyType.PRIVATE, ecSpec);
        byte[] randomKeyData = ecPrivateKeyRandom.getD().toByteArray();
        //random key encryption using 'public from' key
        byte[] encryptedRandomKey;
        try {
            encryptedRandomKey = encryptRandomKey(algorithm, keyPairTo, ecSpec, randomKeyData);
        } catch (IOException e) {
            throw new CommonException(e);
        }

        return new WrapperReEncryptionKey(
                algorithm,
                reEncryptionKey.getReEncryptionKey(),
                ecSpec,
                encryptedRandomKey);
    }

    //TODO refactor
    private static byte[] encryptRandomKey(EncryptionAlgorithm algorithm,
                                           KeyPair keyPairTo,
                                           ECParameterSpec ecSpec,
                                           byte[] randomKeyData) throws IOException {
        int keySize = KeyUtils.getMessageLength(ecSpec);
        int partsCount = (int) Math.ceil(((double) randomKeyData.length) / keySize);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (int i = 0; i < partsCount; i++) {
            int index = (i + 1) * keySize;
            if (index > randomKeyData.length) {
                index = randomKeyData.length;
            }
            int length = index - i * keySize;
            byte[] part = new byte[keySize];
            System.arraycopy(randomKeyData, i * keySize,
                    part, keySize - length, length);
            byte[] encryptedPart = algorithm.encrypt(
                    keyPairTo.getPublic(), part, new SecureRandom());
            outputStream.write(Ints.toByteArray(length));
            outputStream.write(Ints.toByteArray(encryptedPart.length));
            outputStream.write(encryptedPart);
        }

        return outputStream.toByteArray();
    }

    /**
     * Generate EC key pair
     *
     * @param curve the name of the curve requested
     * @return {@link KeyPair} and {@link ECParameterSpec}
     */
    public KeyPairHolder generateECKeyPair(String curve) {
        ECParameterSpec ecParameterSpec = ECNamedCurveTable.getParameterSpec(curve);
        return generateECKeyPair(ecParameterSpec);
    }

    /**
     * Generate EC key pair
     *
     * @param ecParameterSpec EC parameters
     * @return {@link KeyPair} and {@link ECParameterSpec}
     */
    public KeyPairHolder generateECKeyPair(ECParameterSpec ecParameterSpec) {
        KeyPair keyPair = algorithm.generateECKeyPair(ecParameterSpec);
        return new KeyPairHolder(keyPair, ecParameterSpec);
    }

    /**
     * Generate EC key pair and write it to the pem-file
     *
     * @param filename the file name
     * @param curve    the name of the curve requested
     * @param keyType  key type
     */
    public void generateECKeyPairToPEM(
            String filename,
            String curve,
            KeyType keyType) {
        KeyPairHolder generated = generateECKeyPair(curve);
        try {
            KeyUtils.writeKeyPairToPEM(
                    filename,
                    generated.getKeyPair(),
                    generated.getEcParameterSpec(),
                    keyType);
        } catch (IOException e) {
            throw new CommonException(e);
        }
        LOGGER.info("Key '{}' was generated", filename);
    }

    /**
     * Helper method for getting EC parameters
     *
     * @param curve       curve name
     * @param keyPairFrom first key pair
     * @param keyPairTo   second key pair
     * @return EC parameters
     */
    private static ECParameterSpec getECParameterSpec(
            String curve, KeyPair keyPairFrom, KeyPair keyPairTo) {
        ECKey privateKeyFrom = (ECKey) keyPairFrom.getPrivate();
        ECKey privateKeyTo = (ECKey) keyPairTo.getPrivate();
        ECKey publicKeyTo = (ECKey) keyPairTo.getPublic();
        ECParameterSpec ecSpec = null;
        if (curve != null) {
            ecSpec = ECNamedCurveTable.getParameterSpec(curve);
        }
        if (privateKeyFrom.getParameters() != null) {
            ecSpec = getECParameterSpec(privateKeyFrom, ecSpec);
        }
        if (privateKeyTo != null && privateKeyTo.getParameters() != null) {
            ecSpec = getECParameterSpec(privateKeyTo, ecSpec);
        }
        if (publicKeyTo != null && publicKeyTo.getParameters() != null) {
            ecSpec = getECParameterSpec(publicKeyTo, ecSpec);
        }
        if (ecSpec == null) {
            throw new CommonException("Can not determine the EC parameters");
        }
        return ecSpec;
    }

    private static ECParameterSpec getECParameterSpec(ECKey key, ECParameterSpec ecSpec) {
        if (ecSpec == null) {
            ecSpec = key.getParameters();
        } else if (!ecSpec.equals(key.getParameters())) {
            throw new CommonException("Different EC parameters");
        }
        return ecSpec;
    }
}
