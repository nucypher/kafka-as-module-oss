package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.clients.MessageSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;

/**
 * Configuration for {@link AesMessageSerializer}
 */
public class AesMessageSerializerConfig extends MessageSerDeConfig {

    public static final String PUBLIC_KEY_CONFIG = "encryption.public.key";
    public static final String PUBLIC_KEY_DOC = "Path to the EC public key";

    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "encryption.value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC =
            ProducerConfig.VALUE_SERIALIZER_CLASS_DOC;

    public static final String KEY_SERIALIZER_CLASS_CONFIG = "encryption.key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC =
            ProducerConfig.KEY_SERIALIZER_CLASS_DOC;

    public static final String CACHE_ENCRYPTION_CAPACITY_CONFIG = "cache.encryption.capacity";
    public static final String CACHE_ENCRYPTION_CAPACITY_DOC = "Encryption cache capacity";
    public static final int CACHE_ENCRYPTION_CAPACITY_DEFAULT = 200000;

    public static final String MAX_USING_DEK_CONFIG = "encryption.dek.max.using";
    public static final String MAX_USING_DEK_DOC = "Max number of using each DEK";
    public static final int MAX_USING_DEK_DEFAULT = 1000;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef()
                .define(PUBLIC_KEY_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        PUBLIC_KEY_DOC)
                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArraySerializer.class,
                        ConfigDef.Importance.HIGH,
                        VALUE_SERIALIZER_CLASS_DOC)
                .define(KEY_SERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArraySerializer.class,
                        ConfigDef.Importance.HIGH,
                        KEY_SERIALIZER_CLASS_DOC)
                .define(CACHE_ENCRYPTION_CAPACITY_CONFIG,
                        ConfigDef.Type.INT,
                        CACHE_ENCRYPTION_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CACHE_ENCRYPTION_CAPACITY_DOC)
                .define(MAX_USING_DEK_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_USING_DEK_DEFAULT,
                        ConfigDef.Importance.LOW,
                        MAX_USING_DEK_DOC);
    }

    public AesMessageSerializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public AesMessageSerializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
