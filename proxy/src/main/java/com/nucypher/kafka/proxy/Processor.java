package com.nucypher.kafka.proxy;

import kafka.network.RequestChannel;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Processor thread that have its own selector and read requests from sockets
 */
public class Processor extends Thread implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    private final int id;
    private final Selector selector;
    private final Map<String, Queue<Send>> inFlightSends = new HashMap<>();
    //TODO change Queue to BlockingQueue
    private final ConcurrentHashMap<String, Queue<Send>> updatedSends =
            new ConcurrentHashMap<>();
    private final Queue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
    private final Map<String, Queue<ClientRequest>> requests = new HashMap<>();
    private final List<SocketChannel> socketChannels = new LinkedList<>();
    private final SecurityProtocol securityProtocol;
    private final Time time;
    private final Node thisNode;
    private final InetSocketAddress broker;
    private boolean isStopped;
    private final MessageHandlerRouter router;
    private final ClientBrokerChannelBuilder channelBuilder;

    /**
     * @param id                  processor id
     * @param securityProtocol    security protocol
     * @param maxRequestSize      max request size
     * @param connectionMaxIdleMS connection max idle in milliseconds
     * @param metrics             metrics
     * @param time                time
     * @param localHost           local host
     * @param localPort           local port
     * @param broker              broker address
     * @param channelConfigs      channel configs
     */
    public Processor(int id,
                     SecurityProtocol securityProtocol,
                     int maxRequestSize,
                     long connectionMaxIdleMS,
                     Metrics metrics,
                     Time time,
                     String localHost,
                     int localPort,
                     InetSocketAddress broker,
                     MessageHandlerRouter router,
                     AbstractConfig channelConfigs) throws UnknownHostException {
        this.id = id;
        this.securityProtocol = securityProtocol;
        this.time = time;
        this.router = router;
        this.broker = broker;
        this.thisNode = new Node(0, localHost, localPort);
        setName("processor-" + id);
        channelBuilder = new ClientBrokerChannelBuilder(securityProtocol, channelConfigs);
        Map<String, String> metricTags = new HashMap<>(1);
        metricTags.put("networkProcessor", String.valueOf(id));
        this.selector = new Selector(
                maxRequestSize,
                connectionMaxIdleMS,
                metrics,
                time,
                "proxy",
                metricTags,
                false,
                channelBuilder);
    }

    /**
     * Queue up a new connection for reading
     */
    public void accept(SocketChannel socketChannel) {
        if (isStopped) {
            throw new IllegalStateException("Processor is closed");
        }
        newConnections.add(socketChannel);
        selector.wakeup();
    }

    @Override
    public void run() {
        LOGGER.info("Processor {} was started", id);
        try {
            while (!isInterrupted()) {
                // setup any new connections that have been queued up
                configureNewConnections();
                selector.poll(300);
                handleDisconnections();
                processInFlightSends(updatedSends, false);
                processInFlightSends(inFlightSends, true);
                processCompletedReceives();
            }
        } catch (Exception e) {
            LOGGER.error("Error in processor {}", id, e);
        } finally {
            close();
        }
    }

    private void configureNewConnections() throws IOException {
        while (!newConnections.isEmpty()) {
            SocketChannel socketChannel = newConnections.poll();
            try {
                String id = Utils.id(socketChannel);
                selector.register(id, socketChannel);
                socketChannels.add(socketChannel);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Registered new client connection {}", id);
                }
            } catch (Exception e) {
                SocketAddress remoteAddress = socketChannel.getRemoteAddress();
                // need to close the channel here to avoid a socket leak.
                IOUtils.closeQuietly(socketChannel.socket());
                IOUtils.closeQuietly(socketChannel);
                LOGGER.error("Processor {} closed connection from {}",
                        id, remoteAddress, e);
            }
        }
    }

    private void processInFlightSends(Map<String, Queue<Send>> sends,
                                      boolean removeEmpty) {
        Iterator<Map.Entry<String, Queue<Send>>> iterator =
                sends.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Queue<Send>> entry = iterator.next();
            if (!processInFlightSends(entry.getKey(), entry.getValue()) && removeEmpty) {
                //TODO extract to method and add threadsafe checking for emptiness for updatedSends
                iterator.remove();
            }
        }
    }

    private boolean processInFlightSends(String destination, Queue<Send> sends) {
        Send send = sends.peek();
        if (send == null) {
            return true;
        }
        KafkaChannel channel = selector.channel(destination);
        if (channel == null) {
            return false;
        }
        if (!channel.ready()) {
            return true;
        }
        do {
            if (sendSynchronized(send, channel)) {
                sends.remove();
            } else {
                break;
            }
            send = sends.peek();
        } while (send != null &&
                selector.channel(destination) != null);
        return true;
    }

    private void processCompletedReceives() throws IOException {
        List<NetworkReceive> completedReceives = selector.completedReceives();
        for (NetworkReceive receive : completedReceives) {
            String destination = Utils.getDestination(receive.source());
            KafkaChannel channel = selector.channel(destination);
            if (channel == null && Utils.isToBroker(destination)) {
                channelBuilder.buildBrokerChannelBuilder(destination, selector.channel(receive.source()));
                selector.connect(destination, broker, -1, -1);
                channel = selector.channel(destination);
            } else if (channel == null) {
                requests.remove(destination);
                continue;
            }
            Send send;
            if (Utils.isToBroker(destination)) {
                send = updateRequest(
                        destination,
                        selector.channel(receive.source()),
                        receive);
            } else {
                send = updateReceive(destination, receive);
            }
            if (send == null) {
                continue;
            }

            if (!channel.ready() || !sendSynchronized(send, channel)) {
                Queue<Send> queue = inFlightSends.get(destination);
                if (queue == null) {
                    queue = new LinkedList<>();
                    inFlightSends.put(destination, queue);
                }
                queue.add(send);
            }
        }
    }

    private Send updateRequest(
            String destination,
            KafkaChannel clientChannel,
            NetworkReceive receive) throws IOException {
        ByteBuffer copy = receive.payload().duplicate();
        RequestChannel.Session session = new RequestChannel.Session(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
                        clientChannel.principal().getName()),
                clientChannel.socketAddress());
        RequestChannel.Request request = new RequestChannel.Request(
                0,
                receive.source(),
                session,
                copy,
                time.milliseconds(),
                null,
                securityProtocol);
        short apiKey = request.requestId();
        RequestHeader header = request.header();
        String source = receive.source();
        Queue<ClientRequest> queue = requests.get(source);
        if (queue == null) {
            queue = new LinkedList<>();
            requests.put(source, queue);
        }
        queue.add(new ClientRequest(request.connectionId(), header));
        if (ApiKeys.forId(apiKey) == ApiKeys.PRODUCE) {
            ProduceRequest produceRequest = (ProduceRequest) request.bodyAndSize().request;
            router.enqueueProduceRequest(this, destination, request.header(), produceRequest);
            return null;
        }
        return new NetworkSend(destination, receive.payload());
    }

    private Send updateReceive(
            String destination, NetworkReceive receive) throws IOException {
        ByteBuffer copy = receive.payload().duplicate();
        ResponseHeader responseHeader = ResponseHeader.parse(copy);
        ClientRequest request = requests.get(destination).poll();
        RequestHeader header = request.header;
        String connectionId = request.connectionId;
        short apiKey = header.apiKey();
        short apiVersion = header.apiVersion();
        ApiKeys apiKeys = ApiKeys.forId(apiKey);
        Struct responseBody = apiKeys.responseSchema(apiVersion).read(copy);
        correlate(header, responseHeader);
        Send send;
        if (apiKeys == ApiKeys.METADATA) {
            MetadataResponse response = new MetadataResponse(responseBody);
            response = new MetadataResponse(
                    response.throttleTimeMs(),
                    Collections.singletonList(thisNode),
                    response.clusterId(),
                    response.controller().id(),
                    new ArrayList<>(response.topicMetadata()));
//            send = new RequestOrResponseSend(connectionId, responseHeader, response);
//            send = new TopicMetadataResponse()
            send = response.toSend(connectionId, header);
        } /*else if (ApiKeys.forId(apiKey) == ApiKeys.GROUP_COORDINATOR) {
            GroupCoordinatorResponse response = new GroupCoordinatorResponse(responseBody);
            response = new GroupCoordinatorResponse(response.errorCode(), thisNode);
            send = new ResponseSend(connectionId, responseHeader, response);
        }*/ else if (apiKeys == ApiKeys.FETCH) {
            FetchResponse response = new FetchResponse(responseBody);
            router.enqueueFetchResponse(this, destination, header, response);
            send = null;
        } else {
            send = new NetworkSend(destination, receive.payload());
        }
        return send;
    }

    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException(
                    String.format("Correlation id for response (%s) " +
                                    "does not match request (%s), request header: %s",
                            responseHeader.correlationId(),
                            requestHeader.correlationId(),
                            requestHeader));
    }

    /**
     * Handle any disconnected connections
     */
    private void handleDisconnections() {
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Node {} disconnected.", node);
            }
            processDisconnection(node, entry.getValue());
        }
    }

    /**
     * Post process disconnection of a node
     *
     * @param nodeId Id of the node to be disconnected
     */
    private void processDisconnection(String nodeId, ChannelState disconnectState) {
        String id = Utils.getDestination(nodeId);
        KafkaChannel channel = selector.channel(id);
        if (channel == null || !channel.isConnected()) {
            return;
        }
        if (Utils.isToBroker(nodeId)) {
            switch (disconnectState) {
                case AUTHENTICATE:
                    LOGGER.warn("Connection to node {} terminated during authentication. This may indicate " +
                            "that authentication failed due to invalid credentials.", nodeId);
                    break;
                case NOT_CONNECTED:
                    LOGGER.warn("Connection to node {} could not be established. Broker may not be available.",
                            nodeId);
                    break;
                default:
                    break; // Disconnections in other states are logged at debug level in Selector
            }
            //TODO send error to client
        }
        channel.disconnect();
        selector.close(id);
    }

    @Override
    public void close() {
        if (isStopped) {
            return;
        }
        isStopped = true;
        if (!isInterrupted()) {
            interrupt();
        }
        for (SocketChannel channel : socketChannels) {
            IOUtils.closeQuietly(channel.socket());
            IOUtils.closeQuietly(channel);
        }
        selector.close();
        LOGGER.info("Processor {} is stopped", id);
    }

    /**
     * @param send {@link Send}
     */
    public void send(Send send) {
        if (send == null) {
            return;
        }
        String destination = send.destination();
        KafkaChannel channel = selector.channel(destination);
        if (channel == null) {
            return;
        }
        Queue<Send> queue = new LinkedBlockingQueue<>();
        //one channel - one message handler
        Queue<Send> anotherQueue = updatedSends.putIfAbsent(destination, queue);
        if (anotherQueue != null) {
            queue = anotherQueue;
        }
        if (!queue.isEmpty() ||
                !channel.ready() ||
                !sendSynchronized(send, channel)) {
            queue.add(send);
        }
    }

    private boolean sendSynchronized(Send send, KafkaChannel channel) {
        synchronized (channel) {
            if (!channel.hasSend()) {
                selector.send(send);
                selector.wakeup();
                return true;
            }
        }
        return false;
    }

    /**
     * Client request data
     */
    private static class ClientRequest {

        public final RequestHeader header;
        public final String connectionId;

        /**
         * @param connectionId connection id
         * @param header       request header
         */
        public ClientRequest(String connectionId, RequestHeader header) {
            this.connectionId = connectionId;
            this.header = header;
        }

    }

}