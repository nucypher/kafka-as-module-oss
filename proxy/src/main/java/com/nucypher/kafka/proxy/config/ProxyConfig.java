package com.nucypher.kafka.proxy.config;

import com.nucypher.kafka.proxy.ProxyType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;

/**
 * Proxy configuration
 */
public class ProxyConfig extends AbstractProxyConfig {

    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /**
     * <code>connections.max.idle.ms</code>
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * <code>value.serializer</code>
     */
    public static final String VALUE_SERIALIZER_CLASS_CONFIG = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

    /**
     * <code>value.deserializer</code>
     */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

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
    /**
     * <code>proxy.host</code>
     */
    public static final String PROXY_HOST_CONFIG = "proxy.host";
    public static final String PROXY_HOST_DOC = "Hostname or IP for listening by proxy";
    public static final String DEFAULT_PROXY_HOST = "127.0.0.1";

    /**
     * <code>proxy.port</code>
     */
    public static final String PROXY_PORT_CONFIG = "proxy.port";
    public static final String PROXY_PORT_DOC = "Local port for listening by proxy";
    public static final int DEFAULT_PROXY_PORT = 9192;

    /**
     * <code>proxy.processors.num</code>
     */
    public static final String PROXY_NUM_PROCESSORS_CONFIG = "proxy.num.processors";
    public static final String PROXY_NUM_PROCESSORS_DOC = "Number of processors for connections";
    public static final int DEFAULT_PROXY_NUM_PROCESSORS = 1;

    /**
     * <code>proxy.handlers.num</code>
     */
    public static final String PROXY_NUM_HANDLERS_CONFIG = "proxy.num.handlers";
    public static final String PROXY_NUM_HANDLERS_DOC = "Number of message handlers";
    public static final int DEFAULT_PROXY_NUM_HANDLERS = 1;

    /**
     * <code>zookeeper.connect</code>
     */
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String ZOOKEEPER_CONNECT_DEFAULT = "localhost:2181";
    public static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper host string";

    /**
     * <code>proxy.type</code>
     */
    public static final String PROXY_TYPE = "proxy.type";
    public static final String PROXY_TYPE_DEFAULT = ProxyType.CLIENT.toString();
    public static final String PROXY_TYPE_DOC = "Type of proxy server";

    static {
        CONFIG = getConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArrayDeserializer.class,
                        ConfigDef.Importance.HIGH,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC) //TODO add custom doc
                .define(VALUE_SERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        ByteArraySerializer.class,
                        ConfigDef.Importance.HIGH,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_DOC) //TODO add custom doc
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                // proxy configuration
                .define(PROXY_HOST_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_PROXY_HOST,
                        ConfigDef.Importance.HIGH,
                        PROXY_HOST_DOC)
                .define(PROXY_PORT_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_PROXY_PORT,
                        ConfigDef.Importance.HIGH,
                        PROXY_PORT_DOC)
                .define(PROXY_NUM_PROCESSORS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_PROXY_NUM_PROCESSORS,
                        ConfigDef.Importance.HIGH,
                        PROXY_NUM_PROCESSORS_DOC)
                .define(PROXY_NUM_HANDLERS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_PROXY_NUM_HANDLERS,
                        ConfigDef.Importance.HIGH,
                        PROXY_NUM_HANDLERS_DOC)
                .define(ZOOKEEPER_CONNECT,
                        ConfigDef.Type.STRING,
                        ZOOKEEPER_CONNECT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        ZOOKEEPER_CONNECT_DOC)
                .define(PROXY_TYPE,
                        ConfigDef.Type.STRING,
                        PROXY_TYPE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        PROXY_TYPE_DOC);
    }

    public ProxyConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }
}
