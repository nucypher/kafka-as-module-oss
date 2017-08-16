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

        byte[] iv = new byte[dek.getEncoded().length];
        secureRandom.nextBytes(iv);

        byte[] encryptedData = cipher.encrypt(data, dek, iv);
        Message message = new Message(encryptedData, topic, edek, iv);
        return message.serialize();
    }

    /**
     * Decrypt byte array
     *
     * @param payload encrypted data
     * @return decrypted data
     */
    public byte[] decrypt(byte[] payload) {
        Message message = Message.deserialize(payload);

        byte[] data = message.getPayload();
        byte[] edek = message.getEDEK();
        byte[] iv = message.getIV();
        boolean isComplex = message.isComplex();

        Key dek = keyManager.decryptEDEK(edek, isComplex);
        return cipher.decrypt(data, dek, iv);
    }

    /**
     * Re-encrypt EDEK
     *
     * @param payload data
     * @param reKey   re-encryption key
     * @return re-encrypted data
     */
    public byte[] reEncrypt(byte[] payload, WrapperReEncryptionKey reKey) {
        Message message = Message.deserialize(payload);

        byte[] edek = message.getEDEK();
        String topic = message.getTopic();
        edek = keyManager.reEncryptEDEK(topic, edek, reKey);
        message.setEDEK(edek);
        message.setComplex(reKey.isComplex());
        return message.serialize();
    }

}
