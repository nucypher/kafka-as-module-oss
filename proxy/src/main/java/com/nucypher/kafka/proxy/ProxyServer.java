package com.nucypher.kafka.proxy;

import com.nucypher.kafka.clients.ReEncryptionHandler;
import com.nucypher.kafka.proxy.config.AbstractProxyConfig;
import com.nucypher.kafka.proxy.config.ProxyConfig;
import com.nucypher.kafka.proxy.handler.MessageHandler;
import com.nucypher.kafka.proxy.handler.MessageHandlerRouter;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Proxy server main class
 */
public class ProxyServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyServer.class);
    private static final long DEFAULT_CONNECTION_MAX_IDLE_MS = 9 * 60 * 1000;

    private MessageHandler[] handlers;
    private Processor[] processors;
    private Acceptor acceptor;
    private String serverHost;
    private int port;

    /**
     * Create proxy server
     *
     * @param proxyHost           proxy hostname or ip
     * @param proxyPort           proxy port
     * @param numProcessors       number of processors
     * @param numHandlers         number of handlers
     * @param brokerHost          broker host
     * @param brokerPort          broker port
     * @param securityProtocol    security protocol
     * @param serializer          serializer
     * @param deserializer        deserializer
     * @param reEncryptionHandler re-encryption handler
     * @param configs             configuration
     * @throws IOException when error while opening socket
     */
    public ProxyServer(String proxyHost,
                       int proxyPort,
                       int numProcessors,
                       int numHandlers,
                       String brokerHost,
                       int brokerPort,
                       SecurityProtocol securityProtocol,
                       Serializer<byte[]> serializer,
                       Deserializer<byte[]> deserializer,
                       ReEncryptionHandler reEncryptionHandler,
                       Map<?, ?> configs) throws IOException {
        AbstractProxyConfig config = new AbstractProxyConfig(configs);
        configure(proxyHost,
                proxyPort,
                numProcessors,
                numHandlers,
                new InetSocketAddress(brokerHost, brokerPort),
                DEFAULT_CONNECTION_MAX_IDLE_MS,
                securityProtocol,
                serializer,
                deserializer,
                reEncryptionHandler,
                reEncryptionHandler != null ? ProxyType.BROKER : ProxyType.CLIENT,
                config);
    }

    private void configure(String serverHost,
                           int localPort,
                           int numProcessors,
                           int numHandlers,
                           InetSocketAddress broker,
                           long connectionMaxIdleMS,
                           SecurityProtocol securityProtocol,
                           Serializer<byte[]> serializer,
                           Deserializer<byte[]> deserializer,
                           ReEncryptionHandler reEncryptionHandler,
                           ProxyType proxyType,
                           AbstractConfig configs) throws IOException {
        Metrics metrics = new Metrics();
        Time time = new SystemTime();
        if (serverHost == null) {
            serverHost = ProxyConfig.DEFAULT_PROXY_HOST;
        }

        handlers = new MessageHandler[numHandlers];
        for (int i = 0; i < numHandlers; i++) {
            handlers[i] = new MessageHandler(i);
        }
        MessageHandlerRouter router;
        if (proxyType == ProxyType.CLIENT) {
            //TODO serializer/deserializer are not threadsafe
            router = new MessageHandlerRouter(
                    handlers, serializer, deserializer);
        } else {
            router = new MessageHandlerRouter(
                    handlers, reEncryptionHandler);
        }

        processors = new Processor[numProcessors];
        acceptor = new Acceptor(serverHost, localPort, processors);
        this.port = acceptor.getPort();
        this.serverHost = serverHost;

        //TODO change to ThreadPool with thread auto restart
        for (int i = 0; i < numProcessors; i++) {
            processors[i] = new Processor(
                    i,
                    securityProtocol,
                    NetworkReceive.UNLIMITED,
                    connectionMaxIdleMS,
                    metrics,
                    time,
                    serverHost,
                    port,
                    broker,
                    router,
                    configs);
        }
    }

    /**
     * @param configs configuration
     * @throws IOException when error while opening socket
     */
    @SuppressWarnings("unchecked")
    public ProxyServer(Map<?, ?> configs) throws IOException {
        ProxyConfig config = new ProxyConfig(configs);
        String proxyTypeString = config.getString(ProxyConfig.PROXY_TYPE);
        ProxyType proxyType = ProxyType.valueOf(proxyTypeString.toUpperCase());
        Serializer<byte[]> serializer = null;
        Deserializer<byte[]> deserializer = null;
        ReEncryptionHandler reEncryptionHandler = null;
        if (proxyType == ProxyType.CLIENT) {
            serializer = config.getConfiguredInstance(
                    ProxyConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
            serializer.configure((Map<String, Object>) configs, false);
            deserializer = config.getConfiguredInstance(
                    ProxyConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            deserializer.configure((Map<String, Object>) configs, false);
        } else {
            String zooKeeperHost = config.getString(ProxyConfig.ZOOKEEPER_CONNECT);
            reEncryptionHandler = new ReEncryptionHandler(zooKeeperHost, config);
        }

        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        InetSocketAddress address = addresses.get(0);
        configure(
                config.getString(ProxyConfig.PROXY_HOST_CONFIG),
                config.getInt(ProxyConfig.PROXY_PORT_CONFIG),
                config.getInt(ProxyConfig.PROXY_NUM_PROCESSORS_CONFIG),
                config.getInt(ProxyConfig.PROXY_NUM_HANDLERS_CONFIG),
                address,
                config.getLong(ProxyConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                SecurityProtocol.forName(config.getString(ProxyConfig.SECURITY_PROTOCOL_CONFIG)),
                serializer,
                deserializer,
                reEncryptionHandler,
                proxyType,
                config
        );
    }

    /**
     * Start server
     */
    public void start() {
        for (MessageHandler handler : handlers) {
            handler.start();
        }
        for (Processor processor : processors) {
            processor.start();
        }
        acceptor.start();
        LOGGER.info("Proxy server {}:{} was started", serverHost, port);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(acceptor);
        if (processors != null) {
            for (Processor processor : processors) {
                IOUtils.closeQuietly(processor);
            }
        }
        if (handlers != null) {
            for (MessageHandler handler : handlers) {
                IOUtils.closeQuietly(handler);
            }
        }
        LOGGER.info("Proxy server was stopped");
    }

    /**
     * Start proxy server using configuration file
     *
     * @param args path to the configuration file
     * @throws IOException when error while opening socket
     */
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Configuration file is not specified");
            return;
        }
        Properties properties = new Properties();
        try (InputStream stream = new FileInputStream(args[0])) {
            properties.load(stream);
        }
        final ProxyServer server = new ProxyServer(properties);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.close();
            }
        });
        server.start();
    }
}
