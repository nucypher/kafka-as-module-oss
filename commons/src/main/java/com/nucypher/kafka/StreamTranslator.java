package com.nucypher.kafka;

import java.io.InputStream;
import java.io.OutputStream;

/**
 */
public interface StreamTranslator {
    void translate(InputStream inputStream, OutputStream outputStream);
}
