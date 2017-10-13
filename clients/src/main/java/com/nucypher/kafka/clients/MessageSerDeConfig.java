package com.nucypher.kafka.clients;


import com.nucypher.crypto.impl.ElGamalEncryptionAlgorithm;
import com.nucypher.kafka.cipher.CipherFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Base class for configs for message serializers and deserializers
 */
public class MessageSerDeConfig extends AbstractConfig {

    public static final String DEK_ENCRYPTION_ALGORITHM_CONFIG = "encryption.dek.algorithm";
    public static final String DEK_ENCRYPTION_ALGORITHM_DOC =
            "Encryption algorithm used for DEK encryption";
    public static final String DEK_ENCRYPTION_ALGORITHM_DEFAULT =
            ElGamalEncryptionAlgorithm.class.getCanonicalName();

    public static final String DATA_ENCRYPTION_PROVIDER_CONFIG = "encryption.data.provider";
    public static final String DATA_ENCRYPTION_PROVIDER_DOC = "Provider used for data encryption";
    public static final String DATA_ENCRYPTION_PROVIDER_DEFAULT =
            CipherFactory.CipherProvider.BOUNCY_CASTLE.toString();

    public static final String DATA_ENCRYPTION_TRANFORMATION_CONFIG = "encryption.data.transformation";
    public static final String DATA_ENCRYPTION_TRANFORMATION_DOC = "Transformation used for data encryption";
    public static final String DATA_ENCRYPTION_TRANFORMATION_DEFAULT = "AES/GCM/NoPadding";

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(DEK_ENCRYPTION_ALGORITHM_CONFIG,
                        ConfigDef.Type.STRING,
                        DEK_ENCRYPTION_ALGORITHM_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        DEK_ENCRYPTION_ALGORITHM_DOC)
                .define(DATA_ENCRYPTION_PROVIDER_CONFIG,
                        ConfigDef.Type.STRING,
                        DATA_ENCRYPTION_PROVIDER_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        DATA_ENCRYPTION_PROVIDER_DOC)
                .define(DATA_ENCRYPTION_TRANFORMATION_CONFIG,
                        ConfigDef.Type.STRING,
                        DATA_ENCRYPTION_TRANFORMATION_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        DATA_ENCRYPTION_TRANFORMATION_DOC);
    }

    public MessageSerDeConfig(ConfigDef config, Map<?, ?> props) {
        super(config, props);
    }

}
