package com.nucypher.kafka.clients;


import com.nucypher.kafka.utils.EncryptionAlgorithm;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Base class for configs for granular serializers and deserializers
 */
public class StructuredMessageSerDeConfig extends AbstractConfig {

    public static final String GRANULAR_DATA_ACCESSOR_CONFIG =
            "encryption.granular.data.accessor";
    public static final String GRANULAR_DATA_ACCESSOR_DOC =
            "Structured data accessor for granular encryption";

    //TODO extract and rename
    public static final String ALGORITHM_CONFIG = "encryption.algorithm";
    public static final String ALGORITHM_DOC = "Encryption algorithm used for DEK encryption";
    public static final String ALGORITHM_DEFAULT = EncryptionAlgorithm.ELGAMAL.toString();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(GRANULAR_DATA_ACCESSOR_CONFIG, ConfigDef.Type.CLASS,
                        ConfigDef.Importance.HIGH, GRANULAR_DATA_ACCESSOR_DOC)
                .define(ALGORITHM_CONFIG, ConfigDef.Type.STRING, ALGORITHM_DEFAULT,
                        ConfigDef.Importance.HIGH, ALGORITHM_DOC);
    }

    public StructuredMessageSerDeConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
