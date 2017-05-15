package com.nucypher.kafka.clients.encrypt.aes;

import com.nucypher.kafka.cipher.CryptType;
import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.clients.AbstractAesGcmCipher;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.encrypt.StreamEcnryptor;
import com.nucypher.kafka.utils.AESKeyGenerators;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;

import javax.crypto.NoSuchPaddingException;
import java.security.*;
import java.util.HashMap;
import java.util.Map;

import static com.nucypher.kafka.Constants.KEY_ALGORITHM;
import static com.nucypher.kafka.Constants.KEY_EDEK;
import static com.nucypher.kafka.Constants.KEY_IV;

/**
 *
 */
public class AesGcmCipherEncryptor extends AbstractAesGcmCipher
        implements ByteEncryptor, StreamEcnryptor, ExtraParameters {

    private PublicKey publicKey;
    private final Map<Object, Object> extraParameters = new HashMap<>();

    /**
     *
     * @param algorithm -
     * @param publicKey -
     * @throws InvalidAlgorithmParameterException
     * @throws InvalidKeyException
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws NoSuchProviderException
     */
    public AesGcmCipherEncryptor(EncryptionAlgorithm algorithm, PublicKey publicKey)
            throws InvalidAlgorithmParameterException, InvalidKeyException,
            NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
        super(CryptType.ENCRYPT);
        this.publicKey = publicKey;
        this.key = AESKeyGenerators.generateDEK();

        SecureRandom secureRandom = new SecureRandom();

        // generate random IV for Cipher
        this.IV = new byte[getKey().getEncoded().length];
        secureRandom.nextBytes(IV);

        // prepare extra data for Header map
        this.extraParameters.put(KEY_EDEK, KeyUtils.encryptDEK(algorithm, publicKey, getKey(), secureRandom));
        this.extraParameters.put(KEY_IV, IV);
        this.extraParameters.put(KEY_ALGORITHM, algorithm.toString().getBytes());

        this.init(getKey(), getIV());
    }

    @Override
    public Map<Object, Object> getExtraParameters() {
        return this.extraParameters;
    }

    @Override
    public PrivateKey getPrivateKey() {
        throw new UnsupportedOperationException("PrivateKey is not supported by Encryptor");
    }

    @Override
    public PublicKey getPublicKey() {
        return this.publicKey;
    }
}
