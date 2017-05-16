package com.nucypher.kafka.clients.encrypt;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for {@link AesStructuredMessageSerializer}
 */
public class AesStructuredMessageSerializerConfig extends StructuredMessageSerializerConfig {

    public static final String PUBLIC_KEY_CONFIG = "encryption.public.key";
    public static final String PUBLIC_KEY_DOC = "Path to the EC public key";

    private static ConfigDef config;

    static {
        config = baseConfigDef().define(PUBLIC_KEY_CONFIG,
                ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PUBLIC_KEY_DOC);
    }

    public AesStructuredMessageSerializerConfig(Map<?, ?> props) {
        super(config, props);
    }

}
