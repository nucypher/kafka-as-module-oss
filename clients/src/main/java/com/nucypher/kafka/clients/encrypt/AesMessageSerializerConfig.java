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
                        KEY_SERIALIZER_CLASS_DOC);
    }

    public AesMessageSerializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public AesMessageSerializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
