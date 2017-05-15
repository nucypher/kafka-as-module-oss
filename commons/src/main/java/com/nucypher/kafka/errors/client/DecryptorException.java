package com.nucypher.kafka.errors.client;

import com.nucypher.kafka.errors.CommonException;

/**
 * General Decryptor Exception
 */
public class DecryptorException extends CommonException {

    public DecryptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public DecryptorException(String message) {
        super(message);
    }

    public DecryptorException(Throwable cause) {
        super(cause);
    }

    public DecryptorException() {
        super();
    }
}
