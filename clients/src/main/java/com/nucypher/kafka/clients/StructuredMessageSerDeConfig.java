package com.nucypher.kafka.clients;


import org.apache.kafka.common.config.ConfigDef;

/**
 * Base class for configs for granular serializers and deserializers
 */
public class StructuredMessageSerDeConfig {

    public static final String GRANULAR_DATA_ACCESSOR_CONFIG =
            "encryption.granular.data.accessor";
    public static final String GRANULAR_DATA_ACCESSOR_DOC =
            "Structured data accessor for granular encryption";


    public static ConfigDef addGranularConfigDef(ConfigDef configDef) {
        return configDef
                .define(GRANULAR_DATA_ACCESSOR_CONFIG, ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH, GRANULAR_DATA_ACCESSOR_DOC);
    }

}
