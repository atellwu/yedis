package com.yeahmobi.yedis.common;

public class YedisNetworkException extends YedisException {

    private static final long serialVersionUID = 1936628372356991877L;

    public YedisNetworkException() {
    }

    public YedisNetworkException(String message) {
        super(message);
    }

    public YedisNetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    public YedisNetworkException(Throwable cause) {
        super(cause);
    }

}
