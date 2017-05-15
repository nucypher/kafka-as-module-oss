package com.nucypher.kafka.zk;

import com.nucypher.kafka.INamed;
import com.nucypher.kafka.errors.CommonException;

/**
 * Encryption type enum
 *
 * @author szotov
 */
public enum EncryptionType implements INamed {

    /**
     * Message is fully encrypted
     */
    FULL("full", (byte) 0),
    /**
     * Message is encrypted in the fields
     */
    GRANULAR("gran", (byte) 1);

    private String shortName;
    private byte code;

    EncryptionType(String shortName, byte code) {
        this.shortName = shortName;
        this.code = code;
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
     * @return byte code
     */
    public byte getCode() {
        return code;
    }

    /**
     * Get encryption type from code
     *
     * @param code byte code
     * @return encryption type
     */
    public static EncryptionType valueOf(byte code) {
        for (EncryptionType type : EncryptionType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        throw new CommonException("No such encryption type with code '%d'", code);
    }

}
