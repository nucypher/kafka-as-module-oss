package com.nucypher.kafka.clients.granular;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The structured data accessor. The implementation must have default constructor
 */
public interface StructuredDataAccessor {


    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    public void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Deserialize data into internal object
     *
     * @param topic topic associated with data
     * @param data  input data
     */
    public void deserialize(String topic, byte[] data);

    /**
     * Serialize internal data into byte array
     *
     * @return serialized data
     */
    public byte[] serialize();

    /**
     * @return all fields which available for encryption
     */
    public Set<String> getAllFields();

    /**
     * @return all encrypted fields and their data
     */
    public Map<String, byte[]> getAllEncrypted();

    /**
     * Get unencrypted fields data
     *
     * @param field input field
     * @return field data
     */
    public byte[] getUnencrypted(String field);

    /**
     * Add encrypted field
     *
     * @param field field name
     * @param data  encrypted data
     */
    public void addEncrypted(String field, byte[] data);

    /**
     * Add unencrypted field
     *
     * @param field field name
     * @param data  non-encrypted data
     */
    public void addUnencrypted(String field, byte[] data);

    /**
     * @return {@code true} if the iteration has more elements
     */
    public boolean hasNext();

    /**
     * Seek to the next element in the iteration
     *
     * @throws NoSuchElementException if the iteration has no more elements
     */
    public void seekToNext() throws NoSuchElementException;

    /**
     * Reset to the first message
     */
    public void reset();

}
