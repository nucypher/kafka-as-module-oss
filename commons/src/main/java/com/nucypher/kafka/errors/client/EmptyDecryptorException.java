package com.nucypher.kafka.errors.client;

/**
 * If ByteDecryptor is not specified
 */
public class EmptyDecryptorException extends DecryptorException {

    public EmptyDecryptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmptyDecryptorException(String message) {
        super(message);
    }

    public EmptyDecryptorException(Throwable cause) {
        super(cause);
    }

    public EmptyDecryptorException() {
        super("ByteDecryptor is not specified");
    }
}
