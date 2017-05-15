package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.clients.ExtraParameters;
import com.nucypher.kafka.clients.Message;
import com.nucypher.kafka.clients.encrypt.aes.AesGcmCipherEncryptor;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.MessageUtils;
import org.apache.kafka.common.serialization.Serializer;

import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;

import static com.nucypher.kafka.Constants.KEY_EDEK;
import static com.nucypher.kafka.Constants.KEY_IV;

/**
 * P - is payload type
 */
public class AesMessageEncryptorSerializer<P> extends ByteEncryptorSerializer<P>
        implements ByteEncryptor, Serializer<P> {

    private ExtraParameters extraParameters;

    /**
     * @param payloadSerializer - Payload serializer
     * @param byteEncryptor     - byteEncryptor
     */
    public AesMessageEncryptorSerializer(Serializer<P> payloadSerializer, ByteEncryptor byteEncryptor, ExtraParameters extraParameters) {
        super(payloadSerializer, byteEncryptor);

        if (extraParameters == null) {
            throw new IllegalArgumentException("ExtraParameters are empty");
        }

        if (extraParameters.getExtraParameters() == null) {
            throw new IllegalArgumentException("ExtraParameters are empty");
        }

        if (extraParameters.getExtraParameters().get(KEY_EDEK) == null) {
            throw new IllegalArgumentException("EDEK is empty from ExtraParameters");
        }

        if (extraParameters.getExtraParameters().get(KEY_IV) == null) {
            throw new IllegalArgumentException("IV is empty from ExtraParameters");
        }

        this.extraParameters = extraParameters;
    }

    /**
     * @param payloadSerializer - Payload serializer
     * @param encryptor         - {@link AesGcmCipherEncryptor}
     */
    public AesMessageEncryptorSerializer(Serializer<P> payloadSerializer, AesGcmCipherEncryptor encryptor) {
        this(payloadSerializer, encryptor, encryptor);
    }

    /**
     * @param payloadSerializer - Payload serializer
     * @param algorithm         encryption algorithm
     * @param publicKey         - EC public key
     */
    public AesMessageEncryptorSerializer(Serializer<P> payloadSerializer, EncryptionAlgorithm algorithm, PublicKey publicKey) {
        this(payloadSerializer, getEncryptor(algorithm, publicKey));
    }

    private static AesGcmCipherEncryptor getEncryptor(EncryptionAlgorithm algorithm, PublicKey publicKey) {
        try {
            return new AesGcmCipherEncryptor(algorithm, publicKey);
        } catch (InvalidAlgorithmParameterException | InvalidKeyException | NoSuchPaddingException |
                NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new CommonException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, P payload) {
        try {
            return new Message<>(
                    this.getSerializer(),
                    this.getByteEncryptor(),
                    MessageUtils.getHeader(topic, extraParameters),
                    payload)
                    .encrypt();
        } catch (IOException ex) {
            throw new CommonException("Unable to ", ex);
        }
    }

}
