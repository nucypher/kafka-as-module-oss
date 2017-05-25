package com.nucypher.kafka.encrypt;

import avro.shaded.com.google.common.primitives.Ints;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Bytes;
import com.nucypher.crypto.bbs98.WrapperBBS98;
import com.nucypher.crypto.elgamal.WrapperElGamalPRE;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AESKeyGenerators;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;
import com.nucypher.kafka.utils.SubkeyGenerator;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.interfaces.ECPublicKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Class for DEK encryption, decryption and re-encryption
 */
public class DataEncryptionKeyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyUtils.class);
    private static final int DEFAULT_CACHE_SIZE = 200000;

    static {
        DefaultProvider.initializeProvider();
    }

    private EncryptionAlgorithm algorithm;
    private PublicKey publicKey;
    private Map<String, PublicKey> publicKeyMap = new HashMap<>();
    private SecureRandom secureRandom;
    //TODO DEKs changing
    private Map<String, Key> deks = new HashMap<>();
    private boolean useDerivedKeys;
    private PrivateKey privateKey;

    private Integer encryptionCacheCapacity;
    private Integer reEncryptionCacheCapacity;
    private Integer decryptionCacheCapacity;
    private LoadingCache<EncryptionCacheKey, byte[]> encryptionCache;
    private LoadingCache<ReEncryptionCacheKey, byte[]> reEncryptionCache;
    private LoadingCache<DecryptionCacheKey, Key> decryptionCache;

    private static class EncryptionCacheKey {
        private byte[] dek;
        private String data;

        public EncryptionCacheKey(byte[] dek, String data) {
            this.dek = dek;
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EncryptionCacheKey that = (EncryptionCacheKey) o;
            return Arrays.equals(dek, that.dek) &&
                    Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(dek), data);
        }
    }

    private static class ReEncryptionCacheKey {
        private byte[] edek;
        private WrapperReEncryptionKey reKey;

        public ReEncryptionCacheKey(byte[] edek, WrapperReEncryptionKey reKey) {
            this.edek = edek;
            this.reKey = reKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReEncryptionCacheKey that = (ReEncryptionCacheKey) o;
            return Arrays.equals(edek, that.edek) &&
                    Objects.equals(reKey, that.reKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(edek), reKey);
        }
    }

    private static class DecryptionCacheKey {
        private byte[] edek;
        private boolean isComplex;

        public DecryptionCacheKey(byte[] edek, boolean isComplex) {
            this.edek = edek;
            this.isComplex = isComplex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DecryptionCacheKey that = (DecryptionCacheKey) o;
            return isComplex == that.isComplex &&
                    Arrays.equals(edek, that.edek);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(edek), isComplex);
        }
    }

    /**
     * Constructor for re-encryption
     */
    public DataEncryptionKeyManager() {

    }

    /**
     * Constructor for re-encryption
     *
     * @param reEncryptionCacheCapacity re-encryption cache capacity
     */
    public DataEncryptionKeyManager(Integer reEncryptionCacheCapacity) {
        this.reEncryptionCacheCapacity = reEncryptionCacheCapacity;
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
        this(algorithm, null, publicKey, secureRandom, false,
                null, null, null);
    }

    /**
     * Constructor for encryption
     *
     * @param algorithm               encryption algorithm
     * @param publicKey               EC public key
     * @param secureRandom            secure random
     * @param encryptionCacheCapacity encryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom,
                                    Integer encryptionCacheCapacity) {
        this(algorithm, null, publicKey, secureRandom, false,
                encryptionCacheCapacity, null, null);
    }

    /**
     * Constructor for encryption
     *
     * @param algorithm               encryption algorithm
     * @param keyPair                 key pair
     * @param secureRandom            secure random
     * @param useDerivedKeys          use derived keys
     * @param encryptionCacheCapacity encryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    KeyPair keyPair,
                                    SecureRandom secureRandom,
                                    boolean useDerivedKeys,
                                    Integer encryptionCacheCapacity) {
        this(algorithm, keyPair.getPrivate(), keyPair.getPublic(), secureRandom, useDerivedKeys,
                encryptionCacheCapacity, null, null);
    }

    /**
     * Constructor for decryption
     *
     * @param algorithm  encryption algorithm
     * @param privateKey EC private key
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm, PrivateKey privateKey) {
        this(algorithm, privateKey, null);
    }

    /**
     * Constructor for decryption
     *
     * @param algorithm               encryption algorithm
     * @param privateKey              EC private key
     * @param decryptionCacheCapacity decryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    Integer decryptionCacheCapacity) {
        this(algorithm, privateKey, null, null, false,
                null, null, decryptionCacheCapacity);
    }

    /**
     * Constructor for tests
     *
     * @param algorithm      encryption algorithm
     * @param privateKey     EC private key
     * @param publicKey      EC public key
     * @param useDerivedKeys use derived keys
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey,
                                    boolean useDerivedKeys) {
        this(algorithm, privateKey, publicKey, new SecureRandom(), useDerivedKeys,
                null, null, null);
    }

    /**
     * Common constructor
     *
     * @param algorithm                 encryption algorithm
     * @param privateKey                EC private key
     * @param publicKey                 EC public key
     * @param secureRandom              secure random
     * @param useDerivedKeys            use derived keys
     * @param encryptionCacheCapacity   encryption cache capacity
     * @param reEncryptionCacheCapacity re-encryption cache capacity
     * @param decryptionCacheCapacity   decryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom,
                                    boolean useDerivedKeys,
                                    Integer encryptionCacheCapacity,
                                    Integer reEncryptionCacheCapacity,
                                    Integer decryptionCacheCapacity) {
        this.privateKey = privateKey;
        this.algorithm = algorithm;
        this.publicKey = publicKey;
        this.secureRandom = secureRandom;
        this.useDerivedKeys = useDerivedKeys;
        this.encryptionCacheCapacity = encryptionCacheCapacity;
        this.reEncryptionCacheCapacity = reEncryptionCacheCapacity;
        this.decryptionCacheCapacity = decryptionCacheCapacity;
    }

    private void initializeEncryptionCache() {
        if (encryptionCache != null) {
            return;
        }
        if (encryptionCacheCapacity == null) {
            encryptionCacheCapacity = DEFAULT_CACHE_SIZE;
        }
        encryptionCache = CacheBuilder.newBuilder()
                .maximumSize(encryptionCacheCapacity)
                .build(new CacheLoader<EncryptionCacheKey, byte[]>() {
                    public byte[] load(EncryptionCacheKey key) {
                        return encryptDEKOperation(key.dek, key.data);
                    }
                });
        LOGGER.debug("Encryption cache was initialized using capacity {}", encryptionCacheCapacity);
    }

    private void initializeReEncryptionCache() {
        if (reEncryptionCache != null) {
            return;
        }
        if (reEncryptionCacheCapacity == null) {
            reEncryptionCacheCapacity = DEFAULT_CACHE_SIZE;
        }
        reEncryptionCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_CACHE_SIZE)
                .build(new CacheLoader<ReEncryptionCacheKey, byte[]>() {
                    public byte[] load(ReEncryptionCacheKey key) {
                        return reEncryptEDEKOperation(key.edek, key.reKey);
                    }
                });
        LOGGER.debug("Re-encryption cache was initialized using capacity {}", reEncryptionCacheCapacity);
    }

    private void initializeDecryptionCache() {
        if (decryptionCache != null) {
            return;
        }
        if (decryptionCacheCapacity == null) {
            decryptionCacheCapacity = DEFAULT_CACHE_SIZE;
        }
        decryptionCache = CacheBuilder.newBuilder()
                .maximumSize(DEFAULT_CACHE_SIZE)
                .build(new CacheLoader<DecryptionCacheKey, Key>() {
                    public Key load(DecryptionCacheKey key) {
                        return decryptEDEKOperation(key.edek, key.isComplex);
                    }
                });
        LOGGER.debug("Decryption cache was initialized using capacity {}", decryptionCacheCapacity);
    }

    /**
     * Encrypt DEK
     *
     * @param dek Data Encryption Key
     * @return EDEK
     */
    public byte[] encryptDEK(Key dek) {
        return encryptDEK(dek, null);
    }

    /**
     * Encrypt DEK
     *
     * @param dek  Data Encryption Key
     * @param data data for encryption used for derivation key
     * @return EDEK
     */
    public byte[] encryptDEK(Key dek, String data) {
        initializeEncryptionCache();
        byte[] dekBytes = dek.getEncoded();
        try {
            return encryptionCache.get(new EncryptionCacheKey(dekBytes, data));
        } catch (ExecutionException e) {
            throw new CommonException(e.getCause());
        }
    }

    private byte[] encryptDEKOperation(byte[] dek, String data) {
        LOGGER.debug("DEK encryption");
        ECPublicKey ecPublicKey = (ECPublicKey) publicKey;
        if (useDerivedKeys) {
            ecPublicKey = getDerivedKey(data);
        }
        ECParameterSpec ecParameterSpec = ecPublicKey.getParameters();
        switch (algorithm) {
            case BBS98:
                return new WrapperBBS98(ecParameterSpec, secureRandom)
                        .encrypt(ecPublicKey, dek);
            case ELGAMAL:
                return new WrapperElGamalPRE(ecParameterSpec, secureRandom)
                        .encrypt(ecPublicKey, dek);
            default:
                throw new CommonException(
                        "Algorithm %s is not available for DEK encryption",
                        algorithm);
        }
    }

    private ECPublicKey getDerivedKey(String data) {
        ECPublicKey ecPublicKey = (ECPublicKey) publicKeyMap.get(data);
        if (ecPublicKey == null) {
            PrivateKey derivedKey = SubkeyGenerator.deriveKey(privateKey, data);
            ecPublicKey = (ECPublicKey) KeyUtils.generatePublicKey(derivedKey);
            publicKeyMap.put(data, ecPublicKey);
        }
        return ecPublicKey;
    }

    /**
     * Get or generate DEK
     *
     * @param data data for getting or generating DEK
     * @return Data Encryption Key
     */
    public Key getDEK(String data) {
        Key dek = deks.get(data);
        if (dek == null) {
            int size = KeyUtils.getMessageLength(((ECPublicKey) publicKey).getParameters());
            dek = AESKeyGenerators.generateDEK(size);
            deks.put(data, dek);
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
        initializeDecryptionCache();
        try {
            return decryptionCache.get(new DecryptionCacheKey(bytes, isComplex));
        } catch (ExecutionException e) {
            throw new CommonException(e.getCause());
        }
    }

    public Key decryptEDEKOperation(byte[] bytes, boolean isComplex) {
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
        LOGGER.debug("EDEK simple decryption");
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
        LOGGER.debug("EDEK complex decryption");
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

        ECParameterSpec ecSpec = ecPrivateKey.getParameters();

        BigInteger randomKeyData =
                decryptRandomKey(algorithm, ecPrivateKey, encryptedRandomKey);
        ECPrivateKey randomKey = (ECPrivateKey) KeyUtils.getPrivateKey(
                ecPrivateKey.getAlgorithm(), randomKeyData, ecSpec);
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
        initializeReEncryptionCache();
        try {
            return reEncryptionCache.get(new ReEncryptionCacheKey(edek, reKey));
        } catch (ExecutionException e) {
            throw new CommonException(e.getCause());
        }
    }

    public byte[] reEncryptEDEKOperation(byte[] edek, WrapperReEncryptionKey reKey) {
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
        LOGGER.debug("EDEK simple re-encryption");
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
        LOGGER.debug("EDEK complex re-encryption");
        //EDEK re-encryption from 'private from' to 'random private' key
        byte[] reEncrypted = reEncryptEDEK(algorithm, reEncryptionKey, ecSpec, edek);
        byte[] encryptedRandomKeyLength = Ints.toByteArray(encryptedRandomKey.length);
        return Bytes.concat(encryptedRandomKeyLength, encryptedRandomKey, reEncrypted);
    }

}
