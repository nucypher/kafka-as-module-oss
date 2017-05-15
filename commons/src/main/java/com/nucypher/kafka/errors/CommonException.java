package com.nucypher.kafka.errors;

/**
 * {@link RuntimeException} wrapper
 */
public class CommonException extends RuntimeException {

    private static final long serialVersionUID = 487684204074816608L;

    /**
     * @param cause  the cause
     * @param format see format in {@link String#format(String, Object...)}
     * @param args   see args in {@link String#format(String, Object...)}
     */
    public CommonException(Throwable cause, String format, Object... args) {
        super(String.format(format, args), cause);
    }

    /**
     * @param format see format in {@link String#format(String, Object...)}
     * @param args   see args in {@link String#format(String, Object...)}
     */
    public CommonException(String format, Object... args) {
        super(String.format(format, args));
    }

    /**
     * See {@link RuntimeException#RuntimeException()}
     */
    public CommonException() {
        super();
    }

    /**
     * See {@link RuntimeException#RuntimeException(String, Throwable, boolean, boolean)}
     */
    protected CommonException(String message, Throwable cause,
                              boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * See {@link RuntimeException#RuntimeException(String, Throwable)}
     */
    public CommonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * See {@link RuntimeException#RuntimeException(String)}
     */
    public CommonException(String message) {
        super(message);
    }

    /**
     * See {@link RuntimeException#RuntimeException(Throwable)}
     */
    public CommonException(Throwable cause) {
        super(cause);
    }
}
