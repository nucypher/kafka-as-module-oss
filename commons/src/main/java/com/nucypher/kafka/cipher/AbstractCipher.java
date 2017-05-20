package com.nucypher.kafka.cipher;

import com.nucypher.kafka.errors.CommonException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Abstract Cipher
 */
public abstract class AbstractCipher {

    /**
     * @return symmetric algorithm name
     */
    public abstract String getSymmetricAlgorithmName();

    /**
     * @return symmetric cipher name
     */
    public abstract String getSymmetricCipherName();

    /**
     * @return security provider name
     */
    public abstract String getSecurityProviderName();

    private Cipher getCipher(boolean isEncryption, Key key, byte[] IV) {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance(getSymmetricCipherName(), getSecurityProviderName());
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException ex) {
            throw new CommonException(
                    ex,
                    "Unable to get instance of Cipher for %s for security provider: %s",
                    getSymmetricCipherName(),
                    getSecurityProviderName());
        }

        SecretKey keyValue = new SecretKeySpec(key.getEncoded(), getSymmetricAlgorithmName());
        AlgorithmParameterSpec IVspec = new IvParameterSpec(IV);

        try {
            cipher.init(isEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE,
                    keyValue, IVspec);
        } catch (InvalidKeyException | InvalidAlgorithmParameterException ex) {
            throw new CommonException("Unable to initialize Cipher", ex);
        }
        return cipher;
    }

    /**
     * Encrypt data using DEK and IV
     *
     * @param data data for encryption
     * @param key  Data Encryption Key
     * @param IV   initialization vector
     * @return encrypted data
     */
    public byte[] encrypt(byte[] data, Key key, byte[] IV) {
        return translate(true, data, key, IV);
    }

    /**
     * Decrypt data using DEK and IV
     *
     * @param data data for decryption
     * @param key  Data Encryption Key
     * @param IV   initialization vector
     * @return decrypted data
     */
    public byte[] decrypt(byte[] data, Key key, byte[] IV) {
        return translate(false, data, key, IV);
    }

    private byte[] translate(boolean isEncryption, byte[] data, Key key, byte[] IV) {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        this.translate(
                isEncryption,
                new ByteArrayInputStream(data),
                byteArrayOutputStream,
                key,
                IV);

        return byteArrayOutputStream.toByteArray();
    }

    private void translate(boolean isEncryption,
                           InputStream inputStream,
                           OutputStream outputStream,
                           Key key,
                           byte[] IV) {

        int cipherBlockSize = key.getEncoded().length;
        Cipher cipher = getCipher(isEncryption, key, IV);

        byte[] buffer;
        byte[] cipherBlock;
        if (isEncryption) {
            buffer = new byte[cipherBlockSize];
            cipherBlock = new byte[cipher.getOutputSize(buffer.length)];
        } else {
            cipherBlock = new byte[cipherBlockSize];
            buffer = new byte[cipher.getOutputSize(cipherBlock.length)];
        }

        int cipherBytes;
        try {

            int noBytes;
            while ((noBytes = inputStream.read(buffer)) != -1) {
                cipherBytes = cipher.update(buffer, 0, noBytes, cipherBlock);
                outputStream.write(cipherBlock, 0, cipherBytes);
            }

            // !!! WARNING !!! always need to call doFinal
            cipherBytes = cipher.doFinal(cipherBlock, 0);
            outputStream.write(cipherBlock, 0, cipherBytes);
            outputStream.flush();

        } catch (IOException | ShortBufferException | BadPaddingException | IllegalBlockSizeException ex) {
            throw new CommonException("Unable to cipher", ex);
        }
    }
}
