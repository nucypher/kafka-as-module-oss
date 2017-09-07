package com.nucypher.kafka.proxy;

import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Router for collection of {@link MessageHandler} threads
 */
public class MessageHandlerRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerRouter.class);

    private final MessageHandler[] availableHandlers;
    private final ConcurrentHashMap<String, MessageHandler> activeHandlers =
            new ConcurrentHashMap<>(); //TODO remove disconnected destination
    private final AtomicInteger currentHandlerIndex = new AtomicInteger(0);
    private final Serializer<byte[]> serializer;
    private final Deserializer<byte[]> deserializer;

    /**
     * @param handlers array of {@link MessageHandler}
     */
    public MessageHandlerRouter(MessageHandler[] handlers,
                                Serializer<byte[]> serializer,
                                Deserializer<byte[]> deserializer) {
        this.availableHandlers = handlers;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * Enqueue {@link ProduceRequest} for handling
     *
     * @param processor      {@link Processor} that has received a request
     * @param destination    destination id
     * @param requestHeader  request header
     * @param produceRequest request
     */
    public void enqueueProduceRequest(Processor processor,
                                      String destination,
                                      RequestHeader requestHeader,
                                      ProduceRequest produceRequest) {
        MessageHandler currentHandler = getMessageHandler(destination);
        MessageHandler.RequestOrResponse request = new MessageHandler.RequestOrResponse(
                processor, destination, requestHeader, produceRequest, serializer);
        currentHandler.enqueue(request);
    }

    /**
     * Enqueue {@link FetchResponse} for handling
     *
     * @param processor      {@link Processor} that has received a response
     * @param destination    destination id
     * @param requestHeader  request header
//     * @param responseHeader response header
     * @param fetchResponse  response
     */
    public void enqueueFetchResponse(Processor processor,
                                     String destination,
                                     RequestHeader requestHeader,
//                                     ResponseHeader responseHeader,
                                     FetchResponse fetchResponse) {
        MessageHandler currentHandler = getMessageHandler(destination);
        MessageHandler.RequestOrResponse response = new MessageHandler.RequestOrResponse(
                processor,
                destination,
                requestHeader,
//                responseHeader,
                fetchResponse,
                deserializer);
        currentHandler.enqueue(response);
    }

    private MessageHandler getMessageHandler(String destination) {
        MessageHandler currentHandler = availableHandlers[currentHandlerIndex.get()];
        MessageHandler handler = activeHandlers.putIfAbsent(destination, currentHandler);
        if (handler == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} was chosen for {}", currentHandler.getName(), destination);
            }
            incrementIndex();
        } else {
            currentHandler = handler;
        }
        return currentHandler;
    }

    private void incrementIndex() {
        int currentValue;
        int newValue;
        do {
            currentValue = currentHandlerIndex.get();
            newValue = (currentValue + 1) % availableHandlers.length;
        } while (!currentHandlerIndex.compareAndSet(currentValue, newValue));
    }
}
