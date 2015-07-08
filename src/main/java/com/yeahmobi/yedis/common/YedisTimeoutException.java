package com.yeahmobi.yedis.common;

public class YedisTimeoutException extends YedisException {

    private static final long serialVersionUID = -7454647511869528468L;

    public YedisTimeoutException() {
    }

    public YedisTimeoutException(String message) {
        super(message);
    }

    public YedisTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public YedisTimeoutException(Throwable cause) {
        super(cause);
    }

}
