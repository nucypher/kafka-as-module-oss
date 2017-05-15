package com.nucypher.kafka.clients;

import com.nucypher.kafka.ByteTranslator;
import com.nucypher.kafka.Constants;
import com.nucypher.kafka.StreamTranslator;
import com.nucypher.kafka.cipher.CryptType;
import com.nucypher.kafka.cipher.ICipher;

import static com.nucypher.kafka.Constants.AES_GCM_NO_PADDING;
import static com.nucypher.kafka.Constants.SECURITY_PROVIDER_NAME;

/**
 * AES GCM Cipher
 */
public abstract class AbstractAesGcmCipher extends AbstractCipher implements ByteTranslator, StreamTranslator, ICipher {

    /**
     * @param type - DECRYPT, ENCRYPT
     */
    public AbstractAesGcmCipher(CryptType type) {
        this.type = type;
    }

    public String getSymmetricAlgorithmName() {
        return Constants.SYMMETRIC_ALGORITHM;
    }

    public String getSymmetricCipherName() {
        return AES_GCM_NO_PADDING;
    }

    public String getSecurityProviderName() {
        return SECURITY_PROVIDER_NAME;
    }

}
