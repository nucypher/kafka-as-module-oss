package com.nucypher.kafka.proxy;

import com.nucypher.kafka.errors.CommonException;

import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;

/**
 * Proxy utils
 */
public class Utils {

    private Utils() {

    }

    /**
     * Get id for channel
     *
     * @param channel channel
     * @return id
     */
    public static String id(SocketChannel channel) {
        return channel.socket().getLocalAddress().getHostAddress() + ":" +
                channel.socket().getLocalPort() + "-" +
                channel.socket().getInetAddress().getHostAddress() + ":" +
                channel.socket().getPort();
    }

    /**
     * Get destination id from source id
     *
     * @param source source id
     * @return destination id
     */
    public static String getDestination(String source) {
        if (source.contains("-broker")) {
            return source.substring(0, source.indexOf("-broker"));
        } else {
            return source + "-broker";
        }
    }

    /**
     * Checks if the destination is broker
     *
     * @param destination destination id
     * @return result of checking
     */
    public static boolean isToBroker(String destination) {
        return destination.endsWith("-broker");
    }

    /**
     * Get field value
     *
     * @param object    object
     * @param fieldName field name
     * @return value
     */
    @SuppressWarnings("unchecked")
    public static <T> T getField(Object object, String fieldName) {
        Field hostNameField;
        try {
            hostNameField = object.getClass().getDeclaredField(fieldName);
            hostNameField.setAccessible(true);
            return (T) hostNameField.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new CommonException(e);
        }
    }
}
