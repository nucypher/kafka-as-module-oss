package com.nucypher.kafka.encrypt;

import avro.shaded.com.google.common.primitives.Ints;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Bytes;
import com.nucypher.crypto.EncryptionAlgorithm;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AESKeyGenerators;
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
import java.security.Key;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Class for DEK encryption, decryption and re-encryption
 */
public class DataEncryptionKeyManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataEncryptionKeyManager.class);
    private static final int DEFAULT_CACHE_SIZE = 200000;

    static {
        DefaultProvider.initializeProvider();
    }

    private EncryptionAlgorithm algorithm;
    private PublicKey publicKey;
    private Map<String, PublicKey> publicKeyMap = new HashMap<>();
    private SecureRandom secureRandom;
    private Integer maxUsingDEK;
    private Map<String, KeyWrapper> deks = new HashMap<>();
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

    private static class KeyWrapper {
        private Key dek;
        private int usedTimes;

        public KeyWrapper(Key dek) {
            this.dek = dek;
            usedTimes = 0;
        }

        public void updateDEK(Key dek) {
            this.dek = dek;
            usedTimes = 0;
        }

        public Key getDEK() {
            return dek;
        }

        public int getUsedTimes() {
            return usedTimes;
        }

        public void incrementUsedTimes() {
            usedTimes++;
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
     * @param maxUsingDEK  max number of using each DEK
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom,
                                    Integer maxUsingDEK) {
        this(algorithm, null, publicKey, secureRandom, false, maxUsingDEK,
                null, null, null);
    }

    /**
     * Constructor for encryption
     *
     * @param algorithm               encryption algorithm
     * @param publicKey               EC public key
     * @param secureRandom            secure random
     * @param maxUsingDEK             max number of using each DEK
     * @param encryptionCacheCapacity encryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom,
                                    Integer maxUsingDEK,
                                    Integer encryptionCacheCapacity) {
        this(algorithm, null, publicKey, secureRandom, false, maxUsingDEK,
                encryptionCacheCapacity, null, null);
    }

    /**
     * Constructor for encryption
     *
     * @param algorithm               encryption algorithm
     * @param keyPair                 key pair
     * @param secureRandom            secure random
     * @param useDerivedKeys          use derived keys
     * @param maxUsingDEK             max number of using each DEK
     * @param encryptionCacheCapacity encryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    KeyPair keyPair,
                                    SecureRandom secureRandom,
                                    boolean useDerivedKeys,
                                    Integer maxUsingDEK,
                                    Integer encryptionCacheCapacity) {
        this(algorithm, keyPair.getPrivate(), keyPair.getPublic(), secureRandom, useDerivedKeys,
                maxUsingDEK, encryptionCacheCapacity, null, null);
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
                null, null, null, decryptionCacheCapacity);
    }

    /**
     * Constructor for tests
     *
     * @param algorithm      encryption algorithm
     * @param privateKey     EC private key
     * @param publicKey      EC public key
     * @param useDerivedKeys use derived keys
     * @param maxUsingDEK    max number of using each DEK
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey,
                                    boolean useDerivedKeys,
                                    Integer maxUsingDEK) {
        this(algorithm, privateKey, publicKey, new SecureRandom(), useDerivedKeys, maxUsingDEK,
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
     * @param maxUsingDEK               max number of using each DEK
     * @param encryptionCacheCapacity   encryption cache capacity
     * @param reEncryptionCacheCapacity re-encryption cache capacity
     * @param decryptionCacheCapacity   decryption cache capacity
     */
    public DataEncryptionKeyManager(EncryptionAlgorithm algorithm,
                                    PrivateKey privateKey,
                                    PublicKey publicKey,
                                    SecureRandom secureRandom,
                                    boolean useDerivedKeys,
                                    Integer maxUsingDEK,
                                    Integer encryptionCacheCapacity,
                                    Integer reEncryptionCacheCapacity,
                                    Integer decryptionCacheCapacity) {
        this.privateKey = privateKey;
        this.algorithm = algorithm;
        this.publicKey = publicKey;
        this.secureRandom = secureRandom;
        this.useDerivedKeys = useDerivedKeys;
        this.maxUsingDEK = maxUsingDEK;
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
        return algorithm.encrypt(ecPublicKey, dek, secureRandom);
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
        KeyWrapper keyWrapper = deks.get(data);
        int size = KeyUtils.getMessageLength(((ECPublicKey) publicKey).getParameters());
        if (keyWrapper == null) {
            Key dek = AESKeyGenerators.generateDEK(size);
            keyWrapper = new KeyWrapper(dek);
            deks.put(data, keyWrapper);
            LOGGER.debug("Key for data '{}' was generated", data);
        } else if (maxUsingDEK != null && keyWrapper.getUsedTimes() >= maxUsingDEK) {
            Key dek = AESKeyGenerators.generateDEK(size);
            keyWrapper.updateDEK(dek);
            LOGGER.debug("Key for data '{}' was updated", data);
        }
        keyWrapper.incrementUsedTimes();
        return keyWrapper.getDEK();
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

    public Key decryptEDEKOperation(byte[] edek, boolean isComplex) {
        byte[] key;
        if (!isComplex) {
            LOGGER.debug("EDEK simple decryption");
            key = algorithm.decrypt(privateKey, edek);
        } else {
            key = decryptReEncryptionEDEK(privateKey, edek);
        }
        return AESKeyGenerators.create(key, Constants.SYMMETRIC_ALGORITHM);
    }

    private byte[] decryptReEncryptionEDEK(PrivateKey privateKey, byte[] bytes) {
        LOGGER.debug("EDEK complex decryption");
        byte[] data = Arrays.copyOfRange(bytes, 0, Ints.BYTES);
        int encryptedRandomKeyLength = Ints.fromByteArray(data);
        byte[] encryptedRandomKey = new byte[encryptedRandomKeyLength];
        byte[] reEncrypted = new byte[bytes.length - encryptedRandomKeyLength - Ints.BYTES];
        System.arraycopy(bytes, Ints.BYTES,
                encryptedRandomKey, 0, encryptedRandomKeyLength);
        System.arraycopy(bytes, Ints.BYTES + encryptedRandomKeyLength,
                reEncrypted, 0, reEncrypted.length);

        BigInteger randomKeyData;
        try {
            randomKeyData = decryptRandomKey(privateKey, encryptedRandomKey);
        } catch (IOException e) {
            throw new CommonException(e);
        }
        ECParameterSpec ecSpec = ((ECPrivateKey) privateKey).getParameters();
        PrivateKey randomKey = KeyUtils.getPrivateKey(
                privateKey.getAlgorithm(), randomKeyData, ecSpec);
        return algorithm.decrypt(randomKey, reEncrypted);
    }

    private BigInteger decryptRandomKey(PrivateKey privateKey, byte[] encryptedRandomKey)
            throws IOException {
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
            byte[] decrypted = algorithm.decrypt(privateKey, data);
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
        EncryptionAlgorithm algorithm = reKey.getAlgorithm();
        BigInteger reEncryptionKey = reKey.getReEncryptionKey();
        ECParameterSpec ecParameterSpec = reKey.getECParameterSpec();
        if (reKey.isSimple()) {
            LOGGER.debug("EDEK simple re-encryption using algorithm '{}'", algorithm.getClass());
            return algorithm.reEncrypt(reEncryptionKey, ecParameterSpec, edek);
        } else {
            LOGGER.debug("EDEK complex re-encryption using algorithm '{}'", algorithm.getClass());
            byte[] encryptedRandomKey = reKey.getEncryptedRandomKey();
            //EDEK re-encryption from 'private from' to 'random private' key
            byte[] reEncrypted = algorithm.reEncrypt(reEncryptionKey, ecParameterSpec, edek);
            byte[] encryptedRandomKeyLength = Ints.toByteArray(encryptedRandomKey.length);
            return Bytes.concat(encryptedRandomKeyLength, encryptedRandomKey, reEncrypted);
        }
    }

}
