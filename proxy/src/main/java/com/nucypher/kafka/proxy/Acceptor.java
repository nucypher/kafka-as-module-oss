package com.nucypher.kafka.proxy;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * Acceptor thread that handles new connections
 */
public class Acceptor extends Thread implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Acceptor.class);

    private final ServerSocketChannel serverSocketChannel;
    private final int port;
    private final Processor[] processors;

    private int currentProcessor = 0;

    /**
     * @param serverHost server host
     * @param port       local port
     * @param processors array of processors
     * @throws IOException when error while opening socket
     */
    public Acceptor(String serverHost, int port, Processor[] processors) throws IOException {
        setName("acceptor");
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(serverHost, port));
        this.port = serverSocketChannel.socket().getLocalPort();
        this.processors = processors;
    }

    /**
     * @return local port
     */
    public int getPort() {
        return port;
    }

    @Override
    public void run() {
        try {
            java.nio.channels.Selector acceptSelector = java.nio.channels.Selector.open();
            serverSocketChannel.register(acceptSelector, SelectionKey.OP_ACCEPT);
            LOGGER.info("Acceptor listens on port {}", port);
            while (!isInterrupted() && serverSocketChannel.isOpen()) {
                if (acceptSelector.select(1000) > 0) {
                    accept(acceptSelector);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error in acceptor thread", e);
        } finally {
            close();
        }
    }

    private void accept(Selector acceptSelector) throws IOException {
        Iterator<SelectionKey> it = acceptSelector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            if (key.isAcceptable()) {
                SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
                socketChannel.configureBlocking(false);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Accepted new client connection {}:{}",
                            socketChannel.socket().getInetAddress().getHostAddress(),
                            socketChannel.socket().getPort());
                }
                processors[currentProcessor].accept(socketChannel);
                currentProcessor = (currentProcessor + 1) % processors.length;
            }
            it.remove();
        }
    }

    @Override
    public void close() {
        if (!isInterrupted()) {
            interrupt();
        }
        IOUtils.closeQuietly(serverSocketChannel);
        LOGGER.info("Acceptor is stopped");
    }
}
