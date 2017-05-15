package com.nucypher.kafka.utils;

import com.nucypher.kafka.INamed;

/**
 * Available encryption algorithms
 */
public enum EncryptionAlgorithm implements INamed {

    /**
     * BBS98 encryption
     */
    BBS98("bbs98"),
    /**
     * ElGamal encryption
     */
    ELGAMAL("elgamal");

    private String shortName;

    EncryptionAlgorithm(String shortName) {
        this.shortName = shortName;
    }

    @Override
    public String getShortName() {
        return shortName;
    }

    @Override
    public String getName() {
        return name();
    }

}
