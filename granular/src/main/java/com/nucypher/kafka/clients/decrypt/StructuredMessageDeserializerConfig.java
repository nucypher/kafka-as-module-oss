package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Map;

/**
 * Configuration for {@link StructuredMessageDeserializer}
 */
public class StructuredMessageDeserializerConfig extends StructuredMessageSerDeConfig {

    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "encryption.value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC =
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC;

    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "encryption.key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC =
            ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC;

    private static ConfigDef CONFIG;

    static {
        CONFIG = StructuredMessageSerDeConfig.baseConfigDef()
                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArrayDeserializer.class,
                        ConfigDef.Importance.HIGH,
                        VALUE_DESERIALIZER_CLASS_DOC)
                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArrayDeserializer.class,
                        ConfigDef.Importance.HIGH,
                        KEY_DESERIALIZER_CLASS_DOC);
    }

    public static ConfigDef baseConfigDef() {
        return CONFIG;
    }

    public StructuredMessageDeserializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public StructuredMessageDeserializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
