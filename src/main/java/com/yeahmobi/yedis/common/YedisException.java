package com.yeahmobi.yedis.common;

public class YedisException extends RuntimeException {

    private static final long serialVersionUID = -6731964051519025372L;

    public YedisException() {
    }

    public YedisException(String message) {
        super(message);
    }

    public YedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public YedisException(Throwable cause) {
        super(cause);
    }

}
