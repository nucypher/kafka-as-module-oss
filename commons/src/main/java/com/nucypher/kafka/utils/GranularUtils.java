package com.nucypher.kafka.utils;

import com.google.common.collect.ImmutableList;
import com.nucypher.kafka.errors.CommonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.nucypher.kafka.utils.StringUtils.isBlank;

/**
 * Utils for granular encryption
 */
public class GranularUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(GranularUtils.class);
    private static final String SEPARATOR = "-";

    /**
     * Separator for fields
     */
    public static final String FIELD_SEPARATOR = ".";
    /**
     * Regex for FIELD_SEPARATOR
     */
    public static final String FIELD_SEPARATOR_REGEX = "\\.";

    /**
     * Parse path into list of fields
     *
     * @param path path to parse
     * @return fields
     */
    public static List<String> parsePath(String path) {
        if (isBlank(path)) {
            return ImmutableList.of();
        }
        return new ArrayList<>(Arrays.asList(path.trim().split(FIELD_SEPARATOR_REGEX)));
    }

    /**
     * Get full field name from intermediate fields
     *
     * @param fields list of fields
     * @return field name
     */
    public static String getFieldName(List<String> fields) {
        if (fields.size() == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(fields.get(0));
        for (int i = 1; i < fields.size(); i++) {
            builder.append(FIELD_SEPARATOR).append(fields.get(i));
        }
        return builder.toString();
    }

    /**
     * Get full field name from intermediate fields
     *
     * @param parentFields list of parent fields
     * @param field        current field
     * @return field name
     */
    public static String getFieldName(List<String> parentFields, String field) {
        if (parentFields.size() == 0) {
            return field;
        }
        StringBuilder builder = new StringBuilder();
        for (String parentField : parentFields) {
            builder.append(parentField).append(FIELD_SEPARATOR);
        }
        builder.append(field);
        return builder.toString();
    }

    /**
     * Get full field name
     *
     * @param parentField parent field name
     * @param field       current field
     * @return field name
     */
    public static String getFieldName(String parentField, String field) {
        if (isBlank(parentField)) {
            return field;
        }
        if (isBlank(field)) {
            return parentField;
        }
        return parentField + FIELD_SEPARATOR + field;
    }

    /**
     * Get list of fields
     *
     * @param parentFields list of parent fields
     * @param field        current field
     * @return new list of all fields
     */
    public static List<String> addField(List<String> parentFields, String field) {
        List<String> result = new ArrayList<>(parentFields);
        result.add(field);
        return result;
    }

    /**
     * Generate private key from root private key and data
     *
     * @param privateKeyPath full path to the private key file
     * @param data           data for generating key
     * @return derived key
     * @throws CommonException if files does not contain right keys or parameters
     */
    public static PrivateKey deriveKeyFromData(String privateKeyPath, String data)
            throws CommonException {
        KeyPair keyPair;
        try {
            keyPair = KeyUtils.getECKeyPairFromPEM(privateKeyPath);
        } catch (IOException e) {
            throw new CommonException(e);
        }
        PrivateKey privateKey = keyPair.getPrivate();
        if (privateKey == null) {
            throw new CommonException("File '%s' must contain private key", privateKeyPath);
        }

        PrivateKey key = SubkeyGenerator.deriveKey(privateKey, data);
        LOGGER.debug("Private keys were generated from {} and {}", privateKeyPath, data);
        return key;
    }

    /**
     * Get field name with channel
     *
     * @param channel channel
     * @param field   field
     * @return field name
     */
    public static String getChannelFieldName(String channel, String field) {
        return channel + SEPARATOR + field;
    }
}
