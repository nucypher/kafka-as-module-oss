package com.nucypher.kafka.clients.decrypt;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for {@link AesStructuredMessageDeserializer}
 */
public class AesStructuredMessageDeserializerConfig extends StructuredMessageDeserializerConfig {

    public static final String PRIVATE_KEY_CONFIG = "encryption.private.key";
    public static final String PRIVATE_KEY_DOC = "Path to the EC private key";

    private static ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef().define(PRIVATE_KEY_CONFIG,
                ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PRIVATE_KEY_DOC);
    }

    public AesStructuredMessageDeserializerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

}
