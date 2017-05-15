package com.nucypher.kafka.clients;

import com.google.common.primitives.Bytes;
import com.nucypher.kafka.cipher.ICipher;
import com.nucypher.kafka.decrypt.ByteDecryptor;
import com.nucypher.kafka.encrypt.ByteEncryptor;
import com.nucypher.kafka.errors.CommonException;
import com.nucypher.kafka.utils.AESKeyGenerators;
import com.nucypher.kafka.utils.ByteUtils;
import com.nucypher.kafka.utils.EncryptionAlgorithm;
import com.nucypher.kafka.utils.KeyUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;

import static com.nucypher.kafka.Constants.*;
import static com.nucypher.kafka.utils.ByteUtils.BYTES_IN_INT;
import static com.nucypher.kafka.utils.ByteUtils.intToByteArray;

/**
 *
 */
@Slf4j
@Setter
@Getter
@ToString
public class Message<T> implements Serializable {

    private Header header;
    private T payload;

    private transient Serializer<T> payloadSerializer;
    private transient ByteEncryptor payloadByteTranslator;
    private transient Deserializer<T> payloadDeserializer;
    private transient ByteDecryptor payloadDecryptor;
    private transient byte[] messageBytes;

    /**
     * For internal using
     */
    protected Message() {

    }

    /**
     * Constructor for encryption and serialization
     *
     * @param payloadSerializer -
     * @param payloadEncryptor  -
     * @param header            -
     * @param payload           -
     */
    public Message(Serializer<T> payloadSerializer, ByteEncryptor payloadEncryptor, Header header, T payload) {
        this.payloadSerializer = payloadSerializer;
        this.payloadByteTranslator = payloadEncryptor;
        this.header = header;
        this.payload = payload;
    }

    /**
     * Constructor for decryption and deserialization
     *
     * @param payloadDeserializer -
     * @param payloadDecryptor -
     * @param messageBytes -
     * @throws IOException -
     * @throws ClassNotFoundException -
     */
    public Message(Deserializer<T> payloadDeserializer, ByteDecryptor payloadDecryptor, byte[] messageBytes)
            throws IOException, ClassNotFoundException {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDecryptor = payloadDecryptor;
        this.messageBytes = messageBytes;
    }

    /**
     * Decrypt and Deserialize payload
     *
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("all")
    public T decrypt() throws IOException, ClassNotFoundException, NoSuchAlgorithmException,
            InvalidKeyException, NoSuchProviderException, InvalidKeySpecException {

        if (this.payloadDeserializer == null) {
            throw new CommonException("Payload deserializer is empty");
        }

        if (this.payloadDecryptor == null) {
            throw new CommonException("Payload decryptor is empty");
        }

        if(messageBytes == null){
            throw new CommonException("Message bytes are empty");
        }

        // #2 extract EDEK from Message bytes
        byte[] headerBytes = this.extractHeader(this.messageBytes);
        Header decryptedHeader = new Header(headerBytes);

        byte[] EDEK = decryptedHeader.getMap().get(KEY_EDEK);
        byte[] IV = decryptedHeader.getMap().get(KEY_IV);
        byte[] keyTypeBytes = decryptedHeader.getMap().get(KEY_IS_COMPLEX);
        boolean isComplex = keyTypeBytes == null ? false :
                ByteUtils.deserialize(keyTypeBytes, Boolean.class);
        EncryptionAlgorithm algorithm = EncryptionAlgorithm.valueOf(
                new String(decryptedHeader.getMap().get(KEY_ALGORITHM)));

        // #3 decrypt EDEK to DEK with Private Key
        final PrivateKey privateKey = ((ICipher) this.payloadDecryptor).getPrivateKey();
        //TODO extract parameter
        byte[] DEK = KeyUtils.decryptEDEK(algorithm, privateKey, EDEK, isComplex);

        // #4 initialize Cipher with decrypted parameres from message DEK IV
        ((ICipher) this.payloadDecryptor).init(
                AESKeyGenerators.create(DEK, SYMMETRIC_ALGORITHM),  // DEK
                IV                                                  // get IV directly from message Header
        );

        // #5 extract payload bytes
        byte[] payloadBytesNonDecrypted = extractPayload(this.messageBytes);

        // #6 decrypt message bytes via Aes GCM Cipher with DEK & dIV
        byte[] dPayload = this.payloadDecryptor.translate(payloadBytesNonDecrypted);

        // #7 deserialize to particular type
        this.payload = this.payloadDeserializer.deserialize(decryptedHeader.getTopic(), dPayload);
        this.header = decryptedHeader;

        return this.payload;
    }

    /**
     * Encrypt Message to byte[] using payloadSerializer and payloadEncryptor
     *
     * @return - byte[]
     * @throws IOException -
     */
    public byte[] encrypt() throws IOException {

        if (this.payloadSerializer == null) {
            throw new CommonException("Payload Serializer is not specified for message encryption");
        }

        if (this.payloadByteTranslator == null) {
            throw new CommonException("Payload encryptor is not specified for message encryption");
        }

        // payload serialize with empty topic - cause topic is inside header
        byte[] payloadSerialized = this.payloadSerializer.serialize("", this.payload);

        // payload encrypt
        byte[] payloadBytesEncrypted = this.payloadByteTranslator.translate(payloadSerialized);

        // header
        byte[] headerBytes = this.header.serialize();

        return combine(headerBytes, payloadBytesEncrypted);
    }

    /**
     * Byte array representation of Message
     * bytes:
     * 1,2,3,4 - int is headerLength -
     * 5,6,7,8 - int is payloadLength
     * 9,...headerLength - header
     * 8+headerLength,...payloadLength - payload
     *
     * @return - byte[] -
     */
    public static byte[] combine(byte[] header, byte[] payload) {
        // TODO extra checks
        byte[] headerLengthBytes = intToByteArray(header.length);
        byte[] payloadLengthBytes = intToByteArray(payload.length);
        return Bytes.concat(headerLengthBytes, payloadLengthBytes, header, payload);
    }

    /**
     * Extract header from message
     *
     * @param messageBytes -
     * @return byte[]
     */
    public static byte[] extractHeader(byte[] messageBytes) {
        byte[] _header = new byte[BYTES_IN_INT];
        System.arraycopy(messageBytes, 0, _header, 0, BYTES_IN_INT);
        int _headerLength = ByteUtils.byteArrayToInt(_header);

        byte[] HEADER_BYTES = new byte[_headerLength];
        System.arraycopy(messageBytes, BYTES_IN_INT * 2, HEADER_BYTES, 0, _headerLength);

        return HEADER_BYTES;
    }

    /**
     * Extract payload byte array from message
     *
     * @param messageBytes -
     * @return byte[]
     */
    public static byte[] extractPayload(byte[] messageBytes) {
        byte[] _header = new byte[BYTES_IN_INT];
        System.arraycopy(messageBytes, 0, _header, 0, BYTES_IN_INT);
        int _headerLength = ByteUtils.byteArrayToInt(_header);

        byte[] _payload = new byte[BYTES_IN_INT];
        System.arraycopy(messageBytes, BYTES_IN_INT, _payload, 0, BYTES_IN_INT);
        int _payloadLength = ByteUtils.byteArrayToInt(_payload);

        byte[] PAYLOAD_BYTES = new byte[_payloadLength];
        System.arraycopy(messageBytes, BYTES_IN_INT * 2 + _headerLength, PAYLOAD_BYTES, 0, _payloadLength);

        return PAYLOAD_BYTES;
    }

}
