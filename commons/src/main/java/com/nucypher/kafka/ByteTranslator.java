package com.nucypher.kafka;

/**
 *
 */
public interface ByteTranslator {
    byte[] translate(byte[] data);
}
