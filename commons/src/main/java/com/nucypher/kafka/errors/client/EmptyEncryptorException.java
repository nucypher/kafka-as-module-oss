package com.nucypher.kafka.errors.client;

/**
 * If ByteEncryptor is not specified
 */
public class EmptyEncryptorException extends EncryptorException {
    public EmptyEncryptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmptyEncryptorException(String message) {
        super(message);
    }

    public EmptyEncryptorException(Throwable cause) {
        super(cause);
    }

    public EmptyEncryptorException() {
        super("ByteEncryptor is not specified");
    }
}
