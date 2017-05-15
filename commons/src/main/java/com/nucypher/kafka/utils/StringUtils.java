package com.nucypher.kafka.utils;

/**
 *
 */
public class StringUtils {

    /**
     * Is String blank
     * "" - true
     * "  " - true
     * "\n\t" - true
     * "wfwef " - false
     *
     * @param string -
     * @return -
     */
    public static boolean isBlank(String string) {
        return string == null || "".equals(string.trim());
    }

    /**
     * vs to isBlank
     *
     * @param string -
     * @return -
     */
    public static boolean isNotBlank(String string) {
        return !isBlank(string);
    }
}
