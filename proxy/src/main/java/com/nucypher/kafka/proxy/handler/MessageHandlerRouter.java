package com.nucypher.kafka.proxy.handler;

import com.nucypher.kafka.clients.ReEncryptionHandler;
import com.nucypher.kafka.proxy.Processor;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
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
    private Serializer<byte[]> serializer;
    private Deserializer<byte[]> deserializer;
    private ReEncryptionHandler reEncryptionHandler;

    /**
     * @param handlers     array of {@link MessageHandler}
     * @param serializer   serializer
     * @param deserializer deserializer
     */
    public MessageHandlerRouter(MessageHandler[] handlers,
                                Serializer<byte[]> serializer,
                                Deserializer<byte[]> deserializer) {
        this.availableHandlers = handlers;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * @param handlers            array of {@link MessageHandler}
     * @param reEncryptionHandler re-ecnryption handler
     */
    public MessageHandlerRouter(MessageHandler[] handlers,
                                ReEncryptionHandler reEncryptionHandler) {
        this.availableHandlers = handlers;
        this.reEncryptionHandler = reEncryptionHandler;
    }

    /**
     * Enqueue {@link ProduceRequest} for handling
     *
     * @param processor      {@link Processor} that has received a request
     * @param destination    destination id
     * @param requestHeader  request header
     * @param produceRequest request
     * @param principal      Kafka principal
     */
    public void enqueueProduceRequest(Processor processor,
                                      String destination,
                                      RequestHeader requestHeader,
                                      ProduceRequest produceRequest,
                                      String principal) {
        MessageHandler currentHandler = getMessageHandler(destination);
        MessageHandler.RequestOrResponse request;
        if (reEncryptionHandler != null) {
            request = new MessageHandler.RequestOrResponse(
                    processor,
                    destination,
                    requestHeader,
                    produceRequest,
                    reEncryptionHandler,
                    principal);
        } else {
            request = new MessageHandler.RequestOrResponse(
                    processor,
                    destination,
                    requestHeader,
                    produceRequest,
                    serializer,
                    principal);
        }
        currentHandler.enqueue(request);
    }

    /**
     * Enqueue {@link FetchResponse} for handling
     *
     * @param processor     {@link Processor} that has received a response
     * @param destination   destination id
     * @param requestHeader request header
     * @param fetchResponse response
     * @param principal     Kafka principal
     */
    public void enqueueFetchResponse(Processor processor,
                                     String destination,
                                     RequestHeader requestHeader,
                                     FetchResponse fetchResponse,
                                     String principal) {
        MessageHandler currentHandler = getMessageHandler(destination);
        MessageHandler.RequestOrResponse response;
        if (reEncryptionHandler != null) {
            response = new MessageHandler.RequestOrResponse(
                    processor,
                    destination,
                    requestHeader,
                    fetchResponse,
                    reEncryptionHandler,
                    principal);
        } else {
            response = new MessageHandler.RequestOrResponse(
                    processor,
                    destination,
                    requestHeader,
                    fetchResponse,
                    deserializer,
                    principal);
        }
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
