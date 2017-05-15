package com.nucypher.kafka.errors.client;

import com.nucypher.kafka.errors.CommonException;

/**
 * General Encryptor exception
 */
public class EncryptorException extends CommonException {
    public EncryptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public EncryptorException(String message) {
        super(message);
    }

    public EncryptorException(Throwable cause) {
        super(cause);
    }

    public EncryptorException() {
        super();
    }
}
