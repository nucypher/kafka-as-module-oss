package com.nucypher.kafka.clients;

import com.nucypher.kafka.ByteTranslator;
import com.nucypher.kafka.StreamTranslator;
import com.nucypher.kafka.cipher.CryptType;
import com.nucypher.kafka.cipher.ICipher;
import com.nucypher.kafka.errors.CommonException;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.*;
import java.security.spec.AlgorithmParameterSpec;

import static com.nucypher.kafka.utils.StringUtils.isBlank;

/**
 * Abstract Cipher
 */
public abstract class AbstractCipher implements ByteTranslator, StreamTranslator, ICipher {

    protected Key key;
    protected byte[] IV;
    protected CryptType type;
    protected Cipher cipher;
    protected boolean isInitialized = false;
    protected int cipherBlockSize;

    public abstract String getSymmetricAlgorithmName();

    public abstract String getSymmetricCipherName();

    public abstract String getSecurityProviderName();

    /**
     * PublicKey for Decryptor
     * PrivateKey for Encryptor
     *
     * @return -
     */
    @Override
    public Key getKey() {
        return this.key;
    }

    @Override
    public byte[] getIV() {
        return this.IV;
    }

    @Override
    public CryptType getCryptType() {
        return this.type;
    }

    public int getCipherBlockSize() {
        return cipherBlockSize;
    }

    public boolean isCipherInitialized() {
        return this.isInitialized;
    }

    public Cipher getCipher() {
        if (this.cipher == null || !this.isCipherInitialized()) {
            throw new CommonException("Cipher is not initialized");
        }
        return this.cipher;
    }

    @Override
    public AbstractCipher init(Key key, byte[] IV) {

        this.validateInitialization(key, IV);

        this.key = key;
        this.IV = IV;
        this.cipherBlockSize = this.key.getEncoded().length;

        try {
            this.cipher = Cipher.getInstance(getSymmetricCipherName(), getSecurityProviderName());
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException ex) {
            throw new CommonException("Unable to get instance of Cipher for " +
                    getSymmetricCipherName() + " for security provider:" + getSecurityProviderName(), ex);
        }

        // create the key
        SecretKey keyValue = new SecretKeySpec(key.getEncoded(), getSymmetricAlgorithmName());

        // create the IV
        AlgorithmParameterSpec IVspec = new IvParameterSpec(IV);

        // init the cipher
        try {
            this.cipher.init(getCryptType().equals(CryptType.ENCRYPT) ?
                    Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, keyValue, IVspec);
        } catch (InvalidKeyException | InvalidAlgorithmParameterException ex) {
            throw new CommonException("Unable to initialize Cipher", ex);
        }

        this.isInitialized = true;
        return this;
    }

    @Override
    public byte[] translate(byte[] data) {

        if (!isCipherInitialized()) {
            throw new CommonException("Cipher is not initialized");
        }

        // TODO need extra checks for data
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        this.translate(new ByteArrayInputStream(data), byteArrayOutputStream);

        // TODO catch exceptions
        // TODO extra checks for byteArrayOutputStream
        // TODO for debugging
        byte[] outByteArray = byteArrayOutputStream.toByteArray();
        return outByteArray;
    }

    @Override
    public void translate(InputStream inputStream, OutputStream outputStream) {

        if (!isCipherInitialized()) {
            throw new CommonException("Cipher is not initialized");
        }

        // TODO need extra checks

        byte[] buffer;
        byte[] cipherBlock;
        if(type == CryptType.ENCRYPT) {
            buffer = new byte[getCipherBlockSize()];
            cipherBlock = new byte[getCipher().getOutputSize(buffer.length)];
        } else {
            cipherBlock = new byte[getCipherBlockSize()];
            buffer = new byte[getCipher().getOutputSize(cipherBlock.length)];
        }

        int cipherBytes;
        try {

            int noBytes = 0;
            while ((noBytes = inputStream.read(buffer)) != -1) {
                cipherBytes = getCipher().update(buffer, 0, noBytes, cipherBlock);
                outputStream.write(cipherBlock, 0, cipherBytes);
            }

            // !!! WARNING !!! always need to call doFinal
            cipherBytes = getCipher().doFinal(cipherBlock, 0);
            outputStream.write(cipherBlock, 0, cipherBytes);
            outputStream.flush();

        } catch (IOException | ShortBufferException | BadPaddingException | IllegalBlockSizeException ex) {
            throw new CommonException("Unable to cipher", ex);
        }
    }

    /**
     * Validate a bunch of arguments
     *
     * @param key -
     * @param IV  -
     */
    protected void validateInitialization(Key key, byte[] IV) {
        if (key == null) {
            throw new CommonException("Key is empty for Cipher");
        }

        if (IV == null) {
            throw new CommonException("IV vector is empty for Cipher");
        }

        if (isBlank(getSymmetricCipherName())) {
            throw new CommonException("Symmetric cipher name is not specified");
        }

        if (isBlank(getSymmetricAlgorithmName())) {
            throw new CommonException("Symmetric algorithm name is not specified");
        }

        if (isBlank(getSecurityProviderName())) {
            throw new CommonException("Security provider name is not specified");
        }
    }
}
