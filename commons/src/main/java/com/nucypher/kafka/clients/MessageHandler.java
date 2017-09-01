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
        Key dek = keyManager.getDEK(topic);
        byte[] edek = keyManager.encryptDEK(dek, topic);
        Message message = encryptMessage(data, dek);
        message.setEDEK(new EncryptedDataEncryptionKey(edek));
        return message.serialize();
    }

    /**
     * Encrypt byte array
     *
     * @param data  data
     * @param dek   DEK bytes
     * @return encrypted {@link Message}
     */
    public Message encryptMessage(byte[] data, Key dek) {
        byte[] iv = new byte[dek.getEncoded().length];
        secureRandom.nextBytes(iv);

        byte[] encryptedData = cipher.encrypt(data, dek, iv);
        return new Message(encryptedData, iv);
    }

    /**
     * Decrypt byte array
     *
     * @param payload encrypted data
     * @return decrypted data
     */
    public byte[] decrypt(byte[] payload) {
        Message message = Message.deserialize(payload);
        byte[] edek = message.getEDEK().getBytes();
        boolean isComplex = message.getEDEK().isComplex();

        Key dek = keyManager.decryptEDEK(edek, isComplex);
        return decryptMessage(message, dek);
    }

    /**
     * Decrypt message
     *
     * @param message {@link Message}
     * @return decrypted data
     */
    public byte[] decryptMessage(Message message, Key dek) {
        byte[] data = message.getPayload();
        byte[] iv = message.getIV();
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

    /**
     * @return DEK manager
     */
    public DataEncryptionKeyManager getDataEncryptionKeyManager() {
        return keyManager;
    }
}
