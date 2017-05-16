package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Collections;
import java.util.Map;

/**
 * Configuration for {@link StructuredMessageSerializer}
 */
public class StructuredMessageSerializerConfig extends StructuredMessageSerDeConfig {

    public static final String VALUE_SERIALIZER_CLASS_CONFIG = "encryption.value.serializer";
    public static final String VALUE_SERIALIZER_CLASS_DOC =
            ProducerConfig.VALUE_SERIALIZER_CLASS_DOC;

    public static final String KEY_SERIALIZER_CLASS_CONFIG = "encryption.key.serializer";
    public static final String KEY_SERIALIZER_CLASS_DOC =
            ProducerConfig.KEY_SERIALIZER_CLASS_DOC;

    public static final String FIELDS_LIST_CONFIG = "encryption.granular.fields";
    public static final String FIELDS_LIST_DOC = "List of fields for encryption";

    private static ConfigDef CONFIG;

    static {
        CONFIG = StructuredMessageSerDeConfig.baseConfigDef()
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
                .define(FIELDS_LIST_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.HIGH,
                        FIELDS_LIST_DOC);
    }

    public static ConfigDef baseConfigDef() {
        return CONFIG;
    }

    public StructuredMessageSerializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public StructuredMessageSerializerConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
