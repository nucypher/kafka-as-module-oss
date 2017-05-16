package com.nucypher.kafka.zk;

import com.nucypher.kafka.INamed;

/**
 * Client type enum
 *
 * @author szotov
 */
public enum ClientType implements INamed {

    /**
     * Consumer type
     */
    CONSUMER("cons"),
    /**
     * Producer type
     */
    PRODUCER("prod");

    private String shortName;

    ClientType(String shortName) {
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

    /**
     * Check enum contains the value
     *
     * @param value value to check
     * @return result of checking
     */
    public static boolean contains(String value) {
        for (ClientType type : values()) {
            if (type.name().equals(value)) {
                return true;
            }
        }
        return false;
    }

}
