package com.nucypher.kafka.utils;

import com.nucypher.kafka.INamed;

/**
 * Key type
 */
public enum KeyType implements INamed {

    PRIVATE("priv"),
    PUBLIC("pub"),
    PRIVATE_AND_PUBLIC("all"),
    DEFAULT("def");

    private String shortName;

    KeyType(String shortName) {
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
