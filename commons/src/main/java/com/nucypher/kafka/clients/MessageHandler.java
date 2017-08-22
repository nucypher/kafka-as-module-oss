package com.nucypher.kafka.clients;

import com.nucypher.kafka.cipher.AbstractCipher;
import com.nucypher.kafka.encrypt.DataEncryptionKeyManager;
import com.nucypher.kafka.utils.WrapperReEncryptionKey;

import java.security.Key;
import java.security.SecureRandom;

/**
 * Utils for encryption and decryption of byte array
 */
public class MessageHandler {

    private SecureRandom secureRandom;
    private AbstractCipher cipher;
    private DataEncryptionKeyManager keyManager;

    /**
     * Constructor for re-encryption
     */
    public MessageHandler() {
        this.keyManager = new DataEncryptionKeyManager();
    }

    /**
     * Constructor for re-encryption
     *
     * @param keyManager DEK manager
     */
    public MessageHandler(DataEncryptionKeyManager keyManager) {
        this.keyManager = keyManager;
    }

    /**
     * Constructor for encryption and decryption
     *
     * @param cipher       cipher
     * @param keyManager   DEK manager
     * @param secureRandom secure random
     */
    public MessageHandler(AbstractCipher cipher,
                          DataEncryptionKeyManager keyManager,
                          SecureRandom secureRandom) {
        this.cipher = cipher;
        this.keyManager = keyManager;
        this.secureRandom = secureRandom;
    }

    /**
     * Constructor for encryption and decryption
     *
     * @param cipher     cipher
     * @param keyManager DEK manager
     */
    public MessageHandler(AbstractCipher cipher,
                          DataEncryptionKeyManager keyManager) {
        this(cipher, keyManager, new SecureRandom());
    }

    /**
     * Encrypt byte array
     *
     * @param topic topic
     * @param data  data
     * @return encrypted data
     */
    public byte[] encrypt(String topic, byte[] data) {
        return encryptMessage(topic, data).serialize();
    }

    /**
     * Encrypt byte array
     *
     * @param topic topic
     * @param data  data
     * @return encrypted {@link Message}
     */
    public Message encryptMessage(String topic, byte[] data) {
        Key dek = keyManager.getDEK(topic);
        byte[] edek = keyManager.encryptDEK(dek, topic);

        byte[] iv = new byte[dek.getEncoded().length];
        secureRandom.nextBytes(iv);

        byte[] encryptedData = cipher.encrypt(data, dek, iv);
        return new Message(
                encryptedData, iv, new EncryptedDataEncryptionKey(edek));
    }

    /**
     * Decrypt byte array
     *
     * @param payload encrypted data
     * @return decrypted data
     */
    public byte[] decrypt(byte[] payload) {
        Message message = Message.deserialize(payload);
        return decryptMessage(message);
    }

    /**
     * Decrypt message
     *
     * @param message {@link Message}
     * @return decrypted data
     */
    public byte[] decryptMessage(Message message) {
        byte[] data = message.getPayload();
        byte[] edek = message.getEDEK().getBytes();
        byte[] iv = message.getIV();
        boolean isComplex = message.getEDEK().isComplex();

        Key dek = keyManager.decryptEDEK(edek, isComplex);
        return cipher.decrypt(data, dek, iv);
    }

    /**
     * Re-encrypt EDEK
     *
     * @param topic   topic
     * @param payload data
     * @param reKey   re-encryption key
     * @return re-encrypted data
     */
    public byte[] reEncrypt(String topic, byte[] payload, WrapperReEncryptionKey reKey) {
        Message message = Message.deserialize(payload);
        message.setEDEK(reEncryptEDEK(topic, message.getEDEK(), reKey));
        return message.serialize();
    }

    /**
     * Re-encrypt EDEK
     *
     * @param topic topic
     * @param edek  EDEK
     * @param reKey re-encryption key
     * @return re-encrypted EDEK
     */
    public EncryptedDataEncryptionKey reEncryptEDEK(String topic,
                                                    EncryptedDataEncryptionKey edek,
                                                    WrapperReEncryptionKey reKey) {
        byte[] bytes = edek.getBytes();
        bytes = keyManager.reEncryptEDEK(topic, bytes, reKey);
        return new EncryptedDataEncryptionKey(bytes, reKey.isComplex());
    }

}
