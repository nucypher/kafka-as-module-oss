package com.nucypher.kafka.clients.encrypt;

import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Map;

/**
 * Configuration for {@link AesStructuredMessageSerializer}
 */
public class AesStructuredMessageSerializerConfig extends AesMessageSerializerConfig {

    public static final String GRANULAR_DATA_ACCESSOR_CONFIG =
            StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG;

    public static final String FIELDS_LIST_CONFIG = "encryption.granular.fields";
    public static final String FIELDS_LIST_DOC = "List of fields for encryption";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef()
                .define(FIELDS_LIST_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.Importance.HIGH,
                        FIELDS_LIST_DOC);
        StructuredMessageSerDeConfig.addGranularConfigDef(CONFIG);
    }

    public AesStructuredMessageSerializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

}
