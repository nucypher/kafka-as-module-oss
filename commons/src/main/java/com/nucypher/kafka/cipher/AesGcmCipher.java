package com.nucypher.kafka.cipher;

import com.nucypher.kafka.Constants;
import com.nucypher.kafka.DefaultProvider;

/**
 * AES GCM Cipher
 */
public class AesGcmCipher extends AbstractCipher {

    static {
        DefaultProvider.initializeProvider();
    }

    @Override
    public String getSymmetricAlgorithmName() {
        return Constants.SYMMETRIC_ALGORITHM;
    }

    @Override
    public String getSymmetricCipherName() {
        return Constants.AES_GCM_NO_PADDING;
    }

    @Override
    public String getSecurityProviderName() {
        return Constants.SECURITY_PROVIDER_NAME;
    }

}
