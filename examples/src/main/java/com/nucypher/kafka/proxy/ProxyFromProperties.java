package com.nucypher.kafka.proxy;

import com.nucypher.kafka.clients.example.utils.JaasUtils;

import java.io.IOException;
import java.net.URL;

/**
 * Start proxy using configuration file
 */
public class ProxyFromProperties {

    public static void main(String[] args) throws IOException {
        JaasUtils.initializeConfiguration("jaas_proxy.conf");
        URL configs = JaasUtils.class.getClassLoader().getResource("proxy.properties");
        ProxyServer.main(new String[]{configs.getFile().toString()});
    }

}
