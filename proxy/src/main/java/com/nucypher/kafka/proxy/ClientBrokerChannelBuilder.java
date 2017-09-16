package com.nucypher.kafka.proxy;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;

/**
 * Two channel builders: for client and for broker
 */
public class ClientBrokerChannelBuilder implements ChannelBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBrokerChannelBuilder.class);

    private final SecurityProtocol securityProtocol;
    private final AbstractConfig channelConfigs;
    private final Map<String, Object> configValues;
    private final ChannelBuilder clientChannelBuilder;
    //TODO make only one ChannelBuilder for one credentials not for id
    private final Map<String, ChannelBuilder> brokerChannelBuilders = new HashMap<>();

    /**
     * @param securityProtocol security protocol
     * @param channelConfigs   channel configuration
     */
    @SuppressWarnings("unchecked")
    public ClientBrokerChannelBuilder(SecurityProtocol securityProtocol,
                                      AbstractConfig channelConfigs) {
        //TODO extract to method?
        CredentialCache credentialCache = new CredentialCache();
//        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
//                securityProtocol == SecurityProtocol.SASL_SSL) {
//            ScramCredentialUtils.createCache(credentialCache, ScramMechanism.mechanismNames());
//        }
        this.clientChannelBuilder = ChannelBuilders.serverChannelBuilder(
                ListenerName.forSecurityProtocol(securityProtocol),
                securityProtocol,
                channelConfigs,
                credentialCache);
        this.securityProtocol = securityProtocol;
        this.channelConfigs = channelConfigs;
        this.configValues = (Map<String, Object>) channelConfigs.values();
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        throw new KafkaException("Unsupported method");
    }

    @Override
    public KafkaChannel buildChannel(
            String id, SelectionKey key, int maxReceiveSize) throws KafkaException {
        if (Utils.isToBroker(id)) {
            return brokerChannelBuilders.get(id).buildChannel(id, key, maxReceiveSize);
        } else {
            return clientChannelBuilder.buildChannel(id, key, maxReceiveSize);
        }
    }

    @Override
    public void close() {
        try {
            clientChannelBuilder.close();
        } catch (Exception e) {
            LOGGER.warn("Error while closing ChannelBuilder for client", e);
        }
        for (Map.Entry<String, ChannelBuilder> entry :
                brokerChannelBuilders.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing ChannelBuilder [" +
                        entry.getKey() + "] for broker", e);
            }
        }
    }

    /**
     * Create {@link ChannelBuilder} for broker connections
     *
     * @param id      broker channel id
     * @param channel client channel
     */
    public void buildBrokerChannelBuilder(String id, KafkaChannel channel) {
        if (brokerChannelBuilders.get(id) != null) {
            return;
        }

        ChannelBuilder brokerChannelBuilder;
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
                securityProtocol == SecurityProtocol.SASL_SSL) {
            SaslServerAuthenticator authenticator =
                    Utils.getField(channel, "authenticator");
            String clientSaslMechanism = Utils.getField(authenticator, "saslMechanism");
            Password jaasConfig = getJaasConfiguration(clientSaslMechanism, authenticator);
            brokerChannelBuilder = BrokerChannelBuilders.brokerChannelBuilder(
                    securityProtocol,
                    configValues,
                    clientSaslMechanism,
                    jaasConfig);
        } else {
            brokerChannelBuilder = ChannelBuilders.clientChannelBuilder(
                    securityProtocol,
                    JaasContext.Type.SERVER,
                    channelConfigs,
                    ListenerName.forSecurityProtocol(securityProtocol),
                    null,
                    true);
        }
        brokerChannelBuilders.put(id, brokerChannelBuilder);
    }

    private static Password getJaasConfiguration(String mechanism,
                                                 SaslServerAuthenticator authenticator) {
        String configuration;
        switch (mechanism) {
            case "PLAIN":
                String loginModuleName = PlainLoginModule.class.getName();
                String username = authenticator.principal().getName();
                JaasContext jaasContext = Utils.getField(authenticator, "jaasContext");
                String password = null;
                for (AppConfigurationEntry entry : jaasContext.configurationEntries()) {
                    if (entry.getLoginModuleName().equals(loginModuleName)) {
                        password = (String) entry.getOptions().get("user_" + username);
                        break;
                    }
                }
                if (password == null) {
                    throw new KafkaException("Undefined password for user " + username);
                }
                configuration = String.format("%s required username=\"%s\" password=\"%s\";",
                        loginModuleName, username, password);
                break;
//            case "DIGEST-MD5":
//                loginModule = TestDigestLoginModule.class.getName();
//                break;
            default:
//                if (ScramMechanism.isScram(mechanism))
//                    loginModule = ScramLoginModule.class.getName();
//                else
                throw new IllegalArgumentException("Unsupported mechanism " + mechanism);
        }
        return new Password(configuration);
    }

}
