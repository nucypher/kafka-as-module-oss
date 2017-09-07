package com.nucypher.kafka.proxy;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Map;

/**
 * Abstract proxy configuration
 */
public class AbstractProxyConfig extends AbstractConfig {

    private static final ConfigDef CONFIG;

    /**
     * <code>security.protocol</code>
     */
    public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

    /**
     * <code>sasl.enabled.mechanisms</code>
     */
    public static final String SASL_ENABLED_MECHANISMS = SaslConfigs.SASL_ENABLED_MECHANISMS;

    /**
     * <code>sasl.kerberos.principal.to.local.rules</code>
     */
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES =
            SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES;

    static {
        CONFIG = new ConfigDef()
                // security support
                .define(SASL_ENABLED_MECHANISMS,
                        ConfigDef.Type.LIST,
                        SaslConfigs.DEFAULT_SASL_ENABLED_MECHANISMS,
                        ConfigDef.Importance.MEDIUM,
                        SaslConfigs.SASL_ENABLED_MECHANISMS_DOC)
                .define(SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES,
                        ConfigDef.Type.LIST,
                        SaslConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES,
                        ConfigDef.Importance.MEDIUM,
                        SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
                .define(SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();
    }

    public static ConfigDef getConfigDef() {
        return CONFIG;
    }

    public AbstractProxyConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    AbstractProxyConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals, true);
    }
}
