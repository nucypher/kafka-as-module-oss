package com.nucypher.kafka.clients;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.zookeeper.common.PathUtils;

/**
 * Class with {@link ReEncryptionHandler} configuration parameters
 */
public class ReEncryptionHandlerConfigs {

    public static final String RE_ENCRYPTION_KEYS_PATH =
            "reencryption.keys.path";
    public static final String RE_ENCRYPTION_KEYS_PATH_DOC =
            "Root path in ZooKeeper where re-encryption keys are stored";

    public static final String RE_ENCRYPTION_KEYS_CACHE_CAPACITY =
            "reencryption.cache.keys.capacity";
    public static final String RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DOC =
            "Re-encryption keys cache capacity";
    public static final int RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DEFAULT = 100000;

    public static final String RE_ENCRYPTION_KEYS_CACHE_TTL_MS =
            "reencryption.cache.keys.ttl.ms";
    public static final String RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DOC =
            "Re-encryption keys cache TTL in milliseconds";
    public static final long RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DEFAULT = 28800000;

    public static final String GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY =
            "reencryption.cache.keys.granular.capacity";
    public static final String GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DOC =
            "Granular re-encryption keys cache capacity";
    public static final int GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DEFAULT = 1000000;

    public static final String GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS =
            "reencryption.cache.keys.granular.ttl.ms";
    public static final String GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DOC =
            "Granular re-encryption keys cache TTL in milliseconds";
    public static final long GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DEFAULT =
            RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DEFAULT;

    public static final String EDEK_CACHE_CAPACITY = "reencryption.cache.edek.capacity";
    public static final String EDEK_CACHE_CAPACITY_DOC = "EDEK cache capacity";
    public static final int EDEK_CACHE_CAPACITY_DEFAULT =
            RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DEFAULT;

    public static final String CHANNELS_CACHE_CAPACITY = "reencryption.cache.channels.capacity";
    public static final String CHANNELS_CACHE_CAPACITY_DOC = "Channels cache capacity";
    public static final int CHANNELS_CACHE_CAPACITY_DEFAULT = 1000;

    public static final String CHANNELS_CACHE_TTL_MS = "reencryption.cache.channels.ttl.ms";
    public static final String CHANNELS_CACHE_TTL_MS_DOC = "Channels cache TTL in milliseconds";
    public static final long CHANNELS_CACHE_TTL_MS_DEFAULT = 14400000;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(RE_ENCRYPTION_KEYS_PATH,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.HIGH,
                        RE_ENCRYPTION_KEYS_PATH_DOC)
                .define(RE_ENCRYPTION_KEYS_CACHE_CAPACITY,
                        ConfigDef.Type.INT,
                        RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DOC)
                .define(RE_ENCRYPTION_KEYS_CACHE_TTL_MS,
                        ConfigDef.Type.LONG,
                        RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DOC)
                .define(GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY,
                        ConfigDef.Type.INT,
                        GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        GRANULAR_RE_ENCRYPTION_KEYS_CACHE_CAPACITY_DOC)
                .define(GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS,
                        ConfigDef.Type.LONG,
                        GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        GRANULAR_RE_ENCRYPTION_KEYS_CACHE_TTL_MS_DOC)
                .define(EDEK_CACHE_CAPACITY,
                        ConfigDef.Type.INT,
                        EDEK_CACHE_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        EDEK_CACHE_CAPACITY_DOC)
                .define(CHANNELS_CACHE_CAPACITY,
                        ConfigDef.Type.INT,
                        CHANNELS_CACHE_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CHANNELS_CACHE_CAPACITY_DOC)
                .define(CHANNELS_CACHE_TTL_MS,
                        ConfigDef.Type.LONG,
                        CHANNELS_CACHE_TTL_MS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CHANNELS_CACHE_TTL_MS_DOC);
    }

    /**
     * @return {@link ReEncryptionHandler} configuration definition
     */
    public static ConfigDef getConfigDef() {
        return CONFIG;
    }

    /**
     * Validate root path
     *
     * @param keysRootPath keys root path
     * @return result of checking
     */
    public static boolean validatePath(String keysRootPath) {
        try {
            PathUtils.validatePath(keysRootPath);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
