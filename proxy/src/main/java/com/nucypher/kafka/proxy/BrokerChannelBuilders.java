package com.nucypher.kafka.proxy;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.SaslChannelBuilder;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Modified {@link org.apache.kafka.common.network.ChannelBuilders}
 * for creating channels from proxy to broker
 */
public class BrokerChannelBuilders {

    private BrokerChannelBuilders() {
    }

    /**
     * @param securityProtocol    the securityProtocol
     * @param configs             client config
     * @param clientSaslMechanism SASL mechanism
     * @param jaasConfig          JAAS configuration
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `clientSaslMechanism` is not null
     *                                  for SASL security protocol
     */
    public static ChannelBuilder brokerChannelBuilder(SecurityProtocol securityProtocol,
                                                      Map<String, ?> configs,
                                                      String clientSaslMechanism,
                                                      Password jaasConfig) {
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
                securityProtocol == SecurityProtocol.SASL_SSL) {
            if (clientSaslMechanism == null)
                throw new IllegalArgumentException(
                        "`clientSaslMechanism` must be non-null in client mode " +
                                "if `securityProtocol` is `" + securityProtocol + "`");
        }
        return create(securityProtocol, configs, clientSaslMechanism, jaasConfig);
    }

    private static ChannelBuilder create(SecurityProtocol securityProtocol,
                                         Map<String, ?> configs,
                                         String clientSaslMechanism,
                                         Password jaasConfig) {
        Mode mode = Mode.CLIENT;
        ChannelBuilder channelBuilder;
        switch (securityProtocol) {
            case SASL_SSL:
            case SASL_PLAINTEXT:
                Map<String, Password> jaasConfigs = new HashMap<>(1);
                jaasConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
                JaasContext jaasContext = JaasContext.load(
                        JaasContext.Type.CLIENT, null, jaasConfigs);
                channelBuilder = new SaslChannelBuilder(mode, jaasContext, securityProtocol,
                        clientSaslMechanism, true, null);
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }

}
