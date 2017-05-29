package com.nucypher.kafka.clients;


import com.nucypher.crypto.impl.ElGamalEncryptionAlgorithm;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Base class for configs for message serializers and deserializers
 */
public class MessageSerDeConfig extends AbstractConfig {

    public static final String ALGORITHM_CONFIG = "encryption.algorithm";
    public static final String ALGORITHM_DOC = "Encryption algorithm used for DEK encryption";
    public static final String ALGORITHM_DEFAULT =
            ElGamalEncryptionAlgorithm.class.getCanonicalName();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(ALGORITHM_CONFIG, ConfigDef.Type.STRING, ALGORITHM_DEFAULT,
                        ConfigDef.Importance.HIGH, ALGORITHM_DOC);
    }

    public MessageSerDeConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
