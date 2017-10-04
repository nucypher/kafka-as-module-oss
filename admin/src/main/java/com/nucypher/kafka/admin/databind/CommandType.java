package com.nucypher.kafka.admin.databind;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Command type
 */
public enum CommandType {

    /**
     * Generate and save the re-encryption key to the storage
     */
    ADD_KEY,
    /**
     * Delete the re-encryption key from the storage
     */
    DELETE_KEY,
    /**
     * Create the channel in the storage
     */
    ADD_CHANNEL,
    /**
     * Delete the channel from the storage
     */
    DELETE_CHANNEL,
    /**
     * Generate key pair and save it to the file or files
     */
    GENERATE;

    @JsonCreator
    public static CommandType fromString(String key) {
        return key == null ? null :
                CommandType.valueOf(key.toUpperCase());
    }

}
