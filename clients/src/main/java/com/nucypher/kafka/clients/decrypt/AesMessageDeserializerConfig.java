package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.MessageSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Map;

/**
 * Configuration for {@link AesMessageDeserializer}
 */
public class AesMessageDeserializerConfig extends MessageSerDeConfig {

    public static final String PRIVATE_KEY_CONFIG = "encryption.private.key";
    public static final String PRIVATE_KEY_DOC = "Path to the EC private key";

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "encryption.value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC =
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC;

    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "encryption.key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC =
            ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC;

    public static final String CACHE_DECRYPTION_CAPACITY_CONFIG = "cache.decryption.capacity";
    public static final String CACHE_DECRYPTION_CAPACITY_DOC = "Decryption cache capacity";
    public static final int CACHE_DECRYPTION_CAPACITY_DEFAULT = 200000;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef()
                .define(PRIVATE_KEY_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        PRIVATE_KEY_DOC)
                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArrayDeserializer.class,
                        ConfigDef.Importance.HIGH,
                        VALUE_DESERIALIZER_CLASS_DOC)
                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArrayDeserializer.class,
                        ConfigDef.Importance.HIGH,
                        KEY_DESERIALIZER_CLASS_DOC)
                .define(CACHE_DECRYPTION_CAPACITY_CONFIG,
                        ConfigDef.Type.INT,
                        CACHE_DECRYPTION_CAPACITY_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CACHE_DECRYPTION_CAPACITY_DOC);
    }

    public static ConfigDef baseConfigDef() {
        return CONFIG;
    }

    public AesMessageDeserializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public AesMessageDeserializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
