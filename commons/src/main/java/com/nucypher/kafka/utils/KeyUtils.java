package com.nucypher.kafka.utils;

import com.nucypher.crypto.bbs98.BBS98ReEncryptionKeyGenerator;
import com.nucypher.crypto.bbs98.BBS98ReKeyGenParameterSpec;
import com.nucypher.crypto.bbs98.WrapperBBS98;
import com.nucypher.crypto.elgamal.ElGamalReEncryptionKeyGenerator;
import com.nucypher.crypto.elgamal.WrapperElGamalPRE;
import com.nucypher.crypto.interfaces.ReEncryptionKey;
import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.Pair;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.asn1.ASN1OutputStream;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.interfaces.ECKey;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemGenerationException;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemObjectGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

/**
 * Class for working with keys
 *
 * @author szotov
 */
public class KeyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyUtils.class);

    /**
     * Convert {@link ECParameterSpec} to byte array
     *
     * @param ecSpec {@link ECParameterSpec}
     * @return byte array
     * @throws IOException if problem with serializing data
     */
    static byte[] ecSpecToByteArray(ECParameterSpec ecSpec) throws IOException {
        final X9ECParameters params = new X9ECParameters(ecSpec.getCurve(),
                ecSpec.getG(), ecSpec.getN(), ecSpec.getH(), ecSpec.getSeed());
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        ASN1OutputStream output = new ASN1OutputStream(content);
        output.writeObject(params);
        output.flush();
        return content.toByteArray();
    }

    /**
     * Convert {@link X9ECParameters} to {@link ECParameterSpec}
     *
     * @param ecParameters {@link X9ECParameters}
     * @return {@link ECParameterSpec}
     */
    static ECParameterSpec ecParametersToSpec(X9ECParameters ecParameters) {
        return new ECParameterSpec(
                ecParameters.getCurve(), ecParameters.getG(),
                ecParameters.getN(), ecParameters.getH(), ecParameters.getSeed());
    }

    /**
     * Load EC keys from the pem-file
     *
     * @param filename full path to the key file
     * @return keys from file
     * @throws IOException if problem with parsing data
     */
    public static KeyPair getECKeyPairFromPEM(String filename) throws IOException {
        try (PEMParser pemParser = new PEMParser(new FileReader(filename))) {
            Object object = pemParser.readObject();
            while (!(object instanceof PEMKeyPair) &&
                    !(object instanceof SubjectPublicKeyInfo) &&
                    !(object instanceof PrivateKeyInfo) &&
                    object != null) {
                object = pemParser.readObject();
            }
            if (object == null) {
                throw new CommonException("Not found key pair in the file '%s'", filename);
            }
            KeyPair keyPair;
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            if (object instanceof PEMKeyPair) {
                PEMKeyPair pemKeyPair = (PEMKeyPair) object;
                keyPair = converter.getKeyPair(pemKeyPair);
            } else if (object instanceof SubjectPublicKeyInfo) {
                SubjectPublicKeyInfo publicKeyInfo = (SubjectPublicKeyInfo) object;
                keyPair = new KeyPair(converter.getPublicKey(publicKeyInfo), null);
            } else {
                PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) object;
                keyPair = new KeyPair(null, converter.getPrivateKey(privateKeyInfo));
            }

            LOGGER.debug("Key '{}' was obtained", filename);
            return keyPair;
        }
    }

    /**
     * Generate re-encryption key from two EC private keys
     *
     * @param algorithm encryption algorithm
     * @param from      full path to the first key file
     * @param to        full path to the second key file
     * @param keyTypeTo key type of the second file
     * @param curve     the name of the curve requested. If null then used EC
     *                  parameters from keys
     * @return re-encryption key
     * @throws IOException              if problem with parsing data
     * @throws NoSuchAlgorithmException if problem with generating complex key
     * @throws InvalidKeyException      if problem with generating complex key
     * @throws NoSuchProviderException  if problem with generating complex key
     * @throws CommonException          if files does not contain right keys or parameters
     */
    public static WrapperReEncryptionKey generateReEncryptionKey(
            EncryptionAlgorithm algorithm,
            String from,
            String to,
            KeyType keyTypeTo,
            String curve)
            throws IOException,
            NoSuchAlgorithmException,
            InvalidKeyException,
            NoSuchProviderException,
            CommonException {
        DefaultProvider.initializeProvider();
        KeyPair keyPairFrom = getECKeyPairFromPEM(from);
        KeyPair keyPairTo = getECKeyPairFromPEM(to);

        WrapperReEncryptionKey result =
                generateReEncryptionKey(algorithm, keyPairFrom, keyPairTo, keyTypeTo, curve);
        LOGGER.debug("Re-encryption key '{}, {}' was generated using algorithm {}", from, to, algorithm);
        return result;
    }

    /**
     * Generate re-encryption key from two EC private keys
     *
     * @param algorithm   encryption algorithm
     * @param keyPairFrom the first key pair
     * @param keyPairTo   the second key pair
     * @param keyTypeTo   key type of the second key pair
     * @param curve       the name of the curve requested. If null then used EC
     *                    parameters from keys
     * @return re-encryption key
     * @throws IOException              if problem with parsing data
     * @throws NoSuchAlgorithmException if problem with generating complex key
     * @throws InvalidKeyException      if problem with generating complex key
     * @throws NoSuchProviderException  if problem with generating complex key
     * @throws CommonException          if files does not contain right keys or parameters
     */
    public static WrapperReEncryptionKey generateReEncryptionKey(
            EncryptionAlgorithm algorithm,
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            KeyType keyTypeTo,
            String curve)
            throws IOException,
            NoSuchAlgorithmException,
            InvalidKeyException,
            NoSuchProviderException,
            CommonException {
        if (keyPairFrom.getPrivate() == null) {
            throw new CommonException("First key pair must contain private key");
        }
        ECParameterSpec ecSpec = getECParameterSpec(curve, keyPairFrom, keyPairTo);

        WrapperReEncryptionKey result;
        switch (keyTypeTo) {
            case DEFAULT:
            case PRIVATE_AND_PUBLIC:
                if (keyPairTo.getPrivate() != null) {
                    result = getSimpleReEncryptionKey(algorithm, keyPairFrom, keyPairTo, ecSpec);
                } else {
                    result = getComplexReEncryptionKey(algorithm, keyPairFrom, keyPairTo, ecSpec);
                }
                break;
            case PRIVATE:
                if (keyPairTo.getPrivate() == null) {
                    throw new CommonException(
                            "Second key pair with type '%s' must contain private key", keyTypeTo);
                }
                result = getSimpleReEncryptionKey(algorithm, keyPairFrom, keyPairTo, ecSpec);
                break;
            case PUBLIC:
                if (keyPairTo.getPublic() == null) {
                    throw new CommonException(
                            "Second key pair with type '%s' must contain public key", keyTypeTo);
                }
                result = getComplexReEncryptionKey(algorithm, keyPairFrom, keyPairTo, ecSpec);
                break;
            default:
                throw new CommonException("Unsupported key type '%s'", keyTypeTo);
        }

        LOGGER.debug("Re-encryption key was generated");
        return result;
    }

    private static WrapperReEncryptionKey getComplexReEncryptionKey(
            EncryptionAlgorithm algorithm,
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            ECParameterSpec ecSpec)
            throws NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException {
        LOGGER.debug("Used private and public keys for generating re-encryption key");
        switch (algorithm) {
            default:
                throw new CommonException("Algorithm %s is not available for " +
                        "generating re-encryption key using private and public keys",
                        algorithm);
        }
    }

    private static WrapperReEncryptionKey getSimpleReEncryptionKey(
            EncryptionAlgorithm algorithm,
            KeyPair keyPairFrom,
            KeyPair keyPairTo,
            ECParameterSpec ecSpec) {
        LOGGER.debug("Used private keys for generating re-encryption key");
        switch (algorithm) {
            case BBS98:
                BBS98ReKeyGenParameterSpec params = new BBS98ReKeyGenParameterSpec(
                        ecSpec, keyPairFrom.getPrivate(), keyPairTo.getPrivate());
                BBS98ReEncryptionKeyGenerator bbs98Generator = new BBS98ReEncryptionKeyGenerator();
                try {
                    bbs98Generator.initialize(params);
                } catch (InvalidAlgorithmParameterException e) {
                    //unreachable code
                    throw new CommonException(e);
                }

                ReEncryptionKey key = bbs98Generator.generateReEncryptionKey();
                return new WrapperReEncryptionKey(algorithm, key.getEncoded(), ecSpec);
            case ELGAMAL:
                BigInteger reKey = ElGamalReEncryptionKeyGenerator.generateReEncryptionKey(
                        keyPairFrom.getPrivate(), keyPairTo.getPrivate(), ecSpec);
                return new WrapperReEncryptionKey(algorithm, reKey, ecSpec);
            default:
                throw new CommonException("Algorithm %s is not available for " +
                        "generating re-encryption key using private keys",
                        algorithm);
        }
    }

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

    /**
     * Generate EC key pair
     *
     * @param algorithm encryption algorithm
     * @param curve     the name of the curve requested
     * @throws IOException if problem with serializing data
     */
    public static Pair<KeyPair, ECParameterSpec> generateECKeyPair(
            EncryptionAlgorithm algorithm, String curve)
            throws IOException {
        final ECParameterSpec ecParameterSpec = ECNamedCurveTable.getParameterSpec(curve);
        KeyPair keyPair;
        switch (algorithm) {
            case BBS98:
                WrapperBBS98 wrapperBBS98 = new WrapperBBS98(ecParameterSpec, new SecureRandom());
                try {
                    keyPair = wrapperBBS98.keygen();
                } catch (InvalidAlgorithmParameterException e) {
                    throw new CommonException(e);
                }
                break;
            case ELGAMAL:
                WrapperElGamalPRE wrapperElGamal = new WrapperElGamalPRE(ecParameterSpec, new SecureRandom());
                try {
                    keyPair = wrapperElGamal.keygen();
                } catch (InvalidAlgorithmParameterException e) {
                    throw new CommonException(e);
                }
                break;
            default:
                throw new CommonException(
                        "Algorithm %s is not available for generating EC key pairs",
                        algorithm);
        }
        return new Pair<>(keyPair, ecParameterSpec);
    }

    /**
     * Generate EC key pair and write it to the pem-file
     *
     * @param algorithm encryption algorithm
     * @param filename  the file name
     * @param curve     the name of the curve requested
     * @param keyType   key type
     * @throws IOException if problem with serializing data
     */
    public static void generateECKeyPairToPEM(
            EncryptionAlgorithm algorithm,
            String filename,
            String curve,
            KeyType keyType)
            throws IOException {
        Pair<KeyPair, ECParameterSpec> generated = generateECKeyPair(algorithm, curve);
        writeKeyPairToPEM(filename, generated.getFirst(), generated.getLast(), keyType);
        LOGGER.info("Key '{}' was generated", filename);
    }

    private static void writeKeyPairToPEM(String filename,
                                          KeyPair keyPair,
                                          final ECParameterSpec ecSpec,
                                          KeyType keyType)
            throws IOException {
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(filename))) {
            //custom generator because MiscPEMGenerator can't work with EC parameters
            pemWriter.writeObject(new PemObjectGenerator() {
                @Override
                public PemObject generate() throws PemGenerationException {
                    try {
                        return new PemObject("EC PARAMETERS", ecSpecToByteArray(ecSpec));
                    } catch (IOException e) {
                        throw new PemGenerationException(e.getMessage());
                    }
                }
            });
            switch (keyType) {
                case PRIVATE:
                    pemWriter.writeObject(keyPair.getPrivate());
                    break;
                case PUBLIC:
                    pemWriter.writeObject(keyPair.getPublic());
                    break;
                case PRIVATE_AND_PUBLIC:
                    pemWriter.writeObject(keyPair.getPrivate());
                    pemWriter.writeObject(keyPair.getPublic());
                    break;
                default:
                    pemWriter.writeObject(keyPair);
            }
        }
    }

    /**
     * Write key pair to the PEM
     *
     * @param filename file name
     * @param keyPair  key pair
     * @param keyType  type of key to write
     * @throws IOException if problem with serializing data
     */
    public static void writeKeyPairToPEM(String filename,
                                         KeyPair keyPair,
                                         KeyType keyType)
            throws IOException {
        ECParameterSpec ecSpec = null;
        ECKey privateKey = (ECKey) keyPair.getPrivate();
        ECKey publicKey = (ECKey) keyPair.getPublic();
        if (privateKey != null && privateKey.getParameters() != null) {
            ecSpec = privateKey.getParameters();
        }
        if (publicKey != null && publicKey.getParameters() != null) {
            ecSpec = publicKey.getParameters();
        }
        if (ecSpec == null) {
            throw new CommonException("Can not determine the EC parameters");
        }
        writeKeyPairToPEM(filename, keyPair, ecSpec, keyType);
    }

    /**
     * Encrypt DEK
     *
     * @param algorithm    encryption algorithm
     * @param publicKey    EC public key
     * @param DEK          Data Encryption Key
     * @param secureRandom {@link SecureRandom}
     * @return EDEK
     */
    public static byte[] encryptDEK(
            EncryptionAlgorithm algorithm,
            PublicKey publicKey,
            Key DEK,
            SecureRandom secureRandom)
            throws NoSuchAlgorithmException, InvalidKeyException, NoSuchProviderException {
        ECKey ecKey = (ECKey) publicKey;
        switch (algorithm) {
            case BBS98:
                return new WrapperBBS98(ecKey.getParameters(), secureRandom)
                        .encrypt(publicKey, DEK.getEncoded());
            case ELGAMAL:
                return new WrapperElGamalPRE(ecKey.getParameters(), secureRandom)
                        .encrypt(publicKey, DEK.getEncoded());
            default:
                throw new CommonException(
                        "Algorithm %s is not available for DEK encryption",
                        algorithm);
        }
    }

    /**
     * Decrypt EDEK
     *
     * @param algorithm  encryption algorithm
     * @param privateKey EC private key
     * @param bytes      EDEK
     * @param isComplex  re-encrypted with complex key
     * @return DEK
     */
    public static byte[] decryptEDEK(
            EncryptionAlgorithm algorithm,
            PrivateKey privateKey,
            byte[] bytes,
            boolean isComplex)
            throws NoSuchAlgorithmException, InvalidKeyException,
            NoSuchProviderException, InvalidKeySpecException {
        ECPrivateKey ecPrivateKey = (ECPrivateKey) privateKey;
        if (!isComplex) {
            return decryptEDEK(algorithm, ecPrivateKey, bytes);
        } else {
            return decryptReEncryptionEDEK(algorithm, ecPrivateKey, bytes);
        }
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
            NoSuchProviderException, InvalidKeySpecException {
        LOGGER.debug("Complex decryption");
        switch (algorithm) {
            default:
                throw new CommonException(
                        "Algorithm %s is not available for EDEK decryption",
                        algorithm);
        }
    }

    /**
     * Re-encrypt EDEK
     *
     * @param edek  EDEK to re-encrypt
     * @param reKey re-encryption key
     * @return re-encrypted EDEK
     */
    public static byte[] reEncryptEDEK(byte[] edek, WrapperReEncryptionKey reKey) {
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

    private static byte[] reEncryptEDEK(
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

    //TODO add ElGamal and BBS98
    private static byte[] reEncryptEDEK(
            EncryptionAlgorithm algorithm,
            BigInteger reEncryptionKey,
            ECParameterSpec ecSpec,
            byte[] edek,
            byte[] encryptedRandomKey,
            Integer randomKeyLength) {
        LOGGER.debug("Complex re-encryption");
        switch (algorithm) {
            default:
                throw new CommonException(
                        "Algorithm %s is not available for complex EDEK re-encryption",
                        algorithm);
        }
    }

}
