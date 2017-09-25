package com.nucypher.kafka.proxy;

import com.nucypher.kafka.clients.example.utils.JaasUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Start proxy by creating new instance from code
 */
public class TransparentProxyFromInstance {

    public static void main(String[] args) throws IOException {
        JaasUtils.initializeConfiguration("jaas_proxy.conf");
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                SecurityProtocol.SASL_PLAINTEXT.name);
        configs.put(SaslConfigs.SASL_ENABLED_MECHANISMS,
                Collections.singletonList("PLAIN"));
        final ProxyServer server = new ProxyServer(
                null,
                9192,
                1,
                1,
                "localhost",
                9092,
                SecurityProtocol.SASL_PLAINTEXT,
                new ByteArraySerializer(),
                new ByteArrayDeserializer(),
                null,
                configs);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
    }

}
