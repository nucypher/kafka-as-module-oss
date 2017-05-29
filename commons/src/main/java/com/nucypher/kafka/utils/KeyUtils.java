package com.nucypher.kafka.utils;

import com.nucypher.kafka.DefaultProvider;
import com.nucypher.kafka.errors.CommonException;
import org.bouncycastle.asn1.ASN1OutputStream;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.interfaces.ECKey;
import org.bouncycastle.jce.interfaces.ECPrivateKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;
import org.bouncycastle.jce.spec.ECPublicKeySpec;
import org.bouncycastle.math.ec.ECPoint;
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
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

/**
 * Class for working with keys
 *
 * @author szotov
 */
public class KeyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyUtils.class);

    static {
        DefaultProvider.initializeProvider();
    }

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
     * Write EC key pair to PEM file
     *
     * @param filename file name
     * @param keyPair  EC key pair
     * @param ecSpec   EC parameters
     * @param keyType  type of key for writing
     * @throws IOException if problem with serializing data
     */
    public static void writeKeyPairToPEM(String filename,
                                         KeyPair keyPair,
                                         final ECParameterSpec ecSpec,
                                         KeyType keyType) throws IOException {
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
     * Get encrypted message length. Available only for P-256 and P-521 curves
     *
     * @param ecSpec {@link ECParameterSpec}
     * @return message length
     */
    public static int getMessageLength(ECParameterSpec ecSpec) {
        switch (ecSpec.getN().bitLength()) {
            case 256:
                return 16;
            case 521:
                return 32;
            //TODO maybe change to calculating
            default:
                throw new IllegalArgumentException("Available only for P-256 and P-521 curves");
        }
    }

    /**
     * Generate public key from private key
     *
     * @param privateKey EC private key
     * @return public key
     */
    public static PublicKey generatePublicKey(PrivateKey privateKey) {
        ECPrivateKey ecPrivateKey = (ECPrivateKey) privateKey;
        ECParameterSpec ecParameterSpec = ecPrivateKey.getParameters();

        ECPoint q = ecParameterSpec.getG().multiply(ecPrivateKey.getD());
        return getPublicKey(privateKey.getAlgorithm(), q, ecParameterSpec);
    }

    /**
     * Get instance of EC {@link PrivateKey}
     *
     * @param algorithm       algorithm
     * @param d               D value
     * @param ecParameterSpec EC parameters
     * @return EC {@link PrivateKey}
     */
    public static PrivateKey getPrivateKey(String algorithm,
                                           BigInteger d,
                                           ECParameterSpec ecParameterSpec) {
        ECPrivateKeySpec ecPrivateKeySpec = new ECPrivateKeySpec(d, ecParameterSpec);
        return new BCECPrivateKey(algorithm,
                ecPrivateKeySpec, BouncyCastleProvider.CONFIGURATION);
    }

    /**
     * Get instance of EC {@link PublicKey}
     *
     * @param algorithm       algorithm
     * @param q               Q point
     * @param ecParameterSpec EC parameters
     * @return EC {@link PublicKey}
     */
    public static PublicKey getPublicKey(String algorithm,
                                         ECPoint q,
                                         ECParameterSpec ecParameterSpec) {
        ECPublicKeySpec publicKeySpec = new ECPublicKeySpec(q, ecParameterSpec);
        return new BCECPublicKey(algorithm,
                publicKeySpec, BouncyCastleProvider.CONFIGURATION);
    }
}
