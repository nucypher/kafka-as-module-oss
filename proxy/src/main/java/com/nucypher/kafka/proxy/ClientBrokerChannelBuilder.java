package com.nucypher.kafka.proxy;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.KafkaChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SelectionKey;
import java.util.Map;

/**
 * Two channel builders: for client and for broker
 */
public class ClientBrokerChannelBuilder implements ChannelBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBrokerChannelBuilder.class);

    private ChannelBuilder clientChannelBuilder;
    private ChannelBuilder brokerChannelBuilder;

    public ClientBrokerChannelBuilder(
            ChannelBuilder clientChannelBuilder, ChannelBuilder brokerChannelBuilder) {
        this.clientChannelBuilder = clientChannelBuilder;
        this.brokerChannelBuilder = brokerChannelBuilder;
    }

    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        clientChannelBuilder.configure(configs);
        brokerChannelBuilder.configure(configs);
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize) throws KafkaException {
        if (Utils.isToBroker(id)) {
            return brokerChannelBuilder.buildChannel(id, key, maxReceiveSize);
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
        try {
            brokerChannelBuilder.close();
        } catch (Exception e) {
            LOGGER.warn("Error while closing ChannelBuilder for broker", e);
        }
    }
}
