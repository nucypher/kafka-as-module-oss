package com.nucypher.kafka;

/**
 * Interface for objects with names
 *
 * @author szotov
 */
public interface INamed {

    /**
     * @return object name
     */
    public String getName();

    /**
     * @return short object name
     */
    public String getShortName();

}
