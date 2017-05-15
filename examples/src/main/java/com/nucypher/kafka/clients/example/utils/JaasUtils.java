package com.nucypher.kafka.clients.example.utils;

import com.nucypher.kafka.errors.CommonException;

import java.net.URL;

/**
 * Utils for JAAS configuration
 */
public class JaasUtils {

    /**
     * Initialize JAAS using jaas.conf
     */
    public static void initializeConfiguration() {
        initializeConfiguration("jaas.conf");
    }

    /**
     * Initialize JAAS using configuration
     *
     * @param fileName configuration file
     */
    public static void initializeConfiguration(String fileName) {
        URL jaas = JaasUtils.class.getClassLoader().getResource(fileName);
        if (jaas == null) {
            throw new CommonException("%s file not found", fileName);
        }
        System.setProperty(org.apache.kafka.common.security.JaasUtils.JAVA_LOGIN_CONFIG_PARAM,
                jaas.toExternalForm());
    }

}
