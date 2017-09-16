package com.nucypher.kafka.proxy;

/**
 * Type of proxy-server
 */
public enum ProxyType {

    /**
     * Client proxy. Proxy works as serializer/deserializer
     */
    CLIENT,
    /**
     * Broker proxy. Proxy works as re-encryptor
     */
    BROKER;

}
