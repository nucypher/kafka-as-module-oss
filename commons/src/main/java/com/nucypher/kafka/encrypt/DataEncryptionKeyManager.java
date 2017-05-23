package com.nucypher.kafka.encrypt;

import avro.shaded.com.google.common.primitives.Ints;
import com.google.common.primitives.Bytes;
import com.nucypher.crypto.bbs98.WrapperBBS98;
import com.nucypher.crypto.elgamal.WrapperElGamalPRE;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AESKeyGenerators;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jce.interfaces.ECKey;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for DEK encryption, decryption and re-encryption
 */
public class DataEncryptionKeyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyUtils.class);

    static {
        DefaultProvider.initializeProvider();
    }

    private EncryptionAlgorithm algorithm;
    private PublicKey publicKey;
    private SecureRandom secureRandom;
    private Map<String, Key> deks = new HashMap<>();
    private PrivateKey privateKey;
    //TODO edek-dek cache
    //TODO dek-edek cache
    //TODO edek-edek cache

    /**
     * Constructor for re-encryption
     */
    public DataEncryptionKeyManager() {

    }

    /**
     * Constructor for encryption
     *
     * @param algorithm encryption algorithm
     * @param publicKey EC public key
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm, PublicKey publicKey) {
        this(algorithm, publicKey, new SecureRandom());
    }

    /**
     * Constructor for encryption
     *
     * @param algorithm    encryption algorithm
     * @param publicKey    EC public key
     * @param secureRandom secure random
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom) {
        this(algorithm, null, publicKey, secureRandom);
    }

    /**
     * Constructor for decryption
     *
     * @param algorithm  encryption algorithm
     * @param privateKey EC private key
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm, PrivateKey privateKey) {
        this(algorithm, privateKey, null, null);
    }

    /**
     * @param algorithm    encryption algorithm
     * @param privateKey   EC private key
     * @param publicKey    EC public key
     * @param secureRandom secure random
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom) {
        this.privateKey = privateKey;
        this.algorithm = algorithm;
        this.publicKey = publicKey;
        this.secureRandom = secureRandom;
    }

    /**
     * @param algorithm  encryption algorithm
     * @param privateKey EC private key
     * @param publicKey  EC public key
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey) {
        this(algorithm, privateKey, publicKey, new SecureRandom());
    }

    /**
     * Encrypt DEK
     *
     * @param dek Data Encryption Key
     * @return EDEK
     */
    public byte[] encryptDEK(Key dek) {
        ECKey ecKey = (ECKey) publicKey;
        switch (algorithm) {
            case BBS98:
                return new WrapperBBS98(ecKey.getParameters(), secureRandom)
                        .encrypt(publicKey, dek.getEncoded());
            case ELGAMAL:
                return new WrapperElGamalPRE(ecKey.getParameters(), secureRandom)
                        .encrypt(publicKey, dek.getEncoded());
            default:
                throw new CommonException(
                        "Algorithm %s is not available for DEK encryption",
                        algorithm);
        }
    }

    /**
     * Get or generate DEK
     *
     * @param seed seed for DEK
     * @return Data Encryption Key
     */
    public Key getDEK(String seed) {
        Key dek = deks.get(seed);
        if (dek == null) {
            int size = KeyUtils.getMessageLength(((ECPublicKey) publicKey).getParameters());
            dek = AESKeyGenerators.generateDEK(size);
            deks.put(seed, dek);
        }
        return dek;
    }

    /**
     * Decrypt EDEK
     *
     * @param bytes     EDEK
     * @param isComplex re-encrypted with complex key
     * @return DEK
     */
    public Key decryptEDEK(byte[] bytes, boolean isComplex) {
        ECPrivateKey ecPrivateKey = (ECPrivateKey) privateKey;
        byte[] key;
        try {
            if (!isComplex) {
                key = decryptEDEK(algorithm, ecPrivateKey, bytes);
            } else {
                key = decryptReEncryptionEDEK(algorithm, ecPrivateKey, bytes);
            }
        } catch (NoSuchAlgorithmException |
                InvalidKeyException |
                NoSuchProviderException |
                InvalidKeySpecException |
                IOException e) {
            throw new CommonException(e);
        }
        return AESKeyGenerators.create(key, Constants.SYMMETRIC_ALGORITHM);
    }

    private static byte[] decryptEDEK(
            EncryptionAlgorithm algorithm, ECPrivateKey ecPrivateKey, byte[] bytes)
            throws NoSuchAlgorithmException, InvalidKeyException,
            InvalidKeySpecException, NoSuchProviderException {
        LOGGER.debug("Simple decryption");
        switch (algorithm) {
            case BBS98:
                WrapperBBS98 wrapperBBS98 = new WrapperBBS98(
                        ecPrivateKey.getParameters(), null);
                return wrapperBBS98.decrypt(ecPrivateKey, bytes);
            case ELGAMAL:
                WrapperElGamalPRE wrapperElGamalPRE = new WrapperElGamalPRE(
                        ecPrivateKey.getParameters(), null);
                return wrapperElGamalPRE.decrypt(ecPrivateKey, bytes);
            default:
                throw new CommonException(
                        "Algorithm %s is not available for EDEK decryption",
                        algorithm);
        }
    }

    private static byte[] decryptReEncryptionEDEK(
            EncryptionAlgorithm algorithm, ECPrivateKey ecPrivateKey, byte[] bytes)
            throws NoSuchAlgorithmException, InvalidKeyException,
            NoSuchProviderException, InvalidKeySpecException, IOException {
        LOGGER.debug("Complex decryption");
        byte[] data = Arrays.copyOfRange(bytes, 0, Ints.BYTES);
        int encryptedRandomKeyLength = Ints.fromByteArray(data);
        //TODO check this for other algorithms
//        int randomKeyLength = bytes[1] - Byte.MIN_VALUE;
        byte[] encryptedRandomKey = new byte[encryptedRandomKeyLength];
        byte[] reEncrypted = new byte[bytes.length - encryptedRandomKeyLength - Ints.BYTES];
        System.arraycopy(bytes, Ints.BYTES,
                encryptedRandomKey, 0, encryptedRandomKeyLength);
        System.arraycopy(bytes, Ints.BYTES + encryptedRandomKeyLength,
                reEncrypted, 0, reEncrypted.length);

        ECParameterSpec ecPec = ecPrivateKey.getParameters();

        BigInteger randomKeyData =
                decryptRandomKey(algorithm, ecPrivateKey, encryptedRandomKey);
        ECPrivateKeySpec ecPrivateKeySpec = new ECPrivateKeySpec(randomKeyData, ecPec);
        ECPrivateKey randomKey = new BCECPrivateKey(ecPrivateKey.getAlgorithm(),
                ecPrivateKeySpec, BouncyCastleProvider.CONFIGURATION);
        return decryptEDEK(algorithm, randomKey, reEncrypted);
    }

    private static BigInteger decryptRandomKey(
            EncryptionAlgorithm algorithm,
            ECPrivateKey ecPrivateKey,
            byte[] encryptedRandomKey)
            throws IOException, NoSuchAlgorithmException, InvalidKeyException,
            InvalidKeySpecException, NoSuchProviderException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(encryptedRandomKey);
        byte[] lengthBytes = new byte[Ints.BYTES];
        byte[] encryptedLengthBytes = new byte[Ints.BYTES];
        while (inputStream.available() > 0) {
            inputStream.read(lengthBytes);
            inputStream.read(encryptedLengthBytes);
            int length = Ints.fromByteArray(lengthBytes);
            int encryptedLength = Ints.fromByteArray(encryptedLengthBytes);
            byte[] data = new byte[encryptedLength];
            inputStream.read(data);
            byte[] decrypted = decryptEDEK(algorithm, ecPrivateKey, data);
            outputStream.write(decrypted, decrypted.length - length, length);
        }

        return new BigInteger(outputStream.toByteArray());
    }

    /**
     * Re-encrypt EDEK
     *
     * @param edek  EDEK to re-encrypt
     * @param reKey re-encryption key
     * @return re-encrypted EDEK
     */
    public byte[] reEncryptEDEK(byte[] edek, WrapperReEncryptionKey reKey) {
        if (reKey.isSimple()) {
            return reEncryptEDEK(
                    reKey.getAlgorithm(),
                    reKey.getReEncryptionKey(),
                    reKey.getECParameterSpec(),
                    edek);
        } else {
            return reEncryptEDEK(
                    reKey.getAlgorithm(),
                    reKey.getReEncryptionKey(),
                    reKey.getECParameterSpec(),
                    edek,
                    reKey.getEncryptedRandomKey(),
                    reKey.getRandomKeyLength());
        }
    }

    private byte[] reEncryptEDEK(
            EncryptionAlgorithm algorithm,
            BigInteger reEncryptionKey,
            ECParameterSpec ecSpec,
            byte[] edek) {
        LOGGER.debug("Simple re-encryption");
        switch (algorithm) {
            case BBS98:
                WrapperBBS98 wrapperBBS98 = new WrapperBBS98(ecSpec, null);
                return wrapperBBS98.reencrypt(reEncryptionKey, edek);
            case ELGAMAL:
                WrapperElGamalPRE wrapperElGamalPRE =
                        new WrapperElGamalPRE(ecSpec, null);
                return wrapperElGamalPRE.reencrypt(reEncryptionKey, edek);
            default:
                throw new CommonException(
                        "Algorithm %s is not available for simple EDEK re-encryption",
                        algorithm);
        }
    }

    private byte[] reEncryptEDEK(
            EncryptionAlgorithm algorithm,
            BigInteger reEncryptionKey,
            ECParameterSpec ecSpec,
            byte[] edek,
            byte[] encryptedRandomKey,
            Integer randomKeyLength) {
        LOGGER.debug("Complex re-encryption");
        //EDEK re-encryption from 'private from' to 'random private' key
        byte[] reEncrypted = reEncryptEDEK(algorithm, reEncryptionKey, ecSpec, edek);
        byte[] encryptedRandomKeyLength = Ints.toByteArray(encryptedRandomKey.length);
        return Bytes.concat(encryptedRandomKeyLength, encryptedRandomKey, reEncrypted);
    }

}
