package com.nucypher.kafka.clients.decrypt;

import com.nucypher.kafka.clients.StructuredMessageSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for {@link AesStructuredMessageDeserializer}
 */
public class AesStructuredMessageDeserializerConfig extends AesMessageDeserializerConfig {

    public static final String GRANULAR_DATA_ACCESSOR_CONFIG =
            StructuredMessageSerDeConfig.GRANULAR_DATA_ACCESSOR_CONFIG;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef();
        StructuredMessageSerDeConfig.addGranularConfigDef(CONFIG);
    }

    public AesStructuredMessageDeserializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

}
