package com.yeahmobi.yedis.async;

public class OperationResult<T> {

    private T v;

    public OperationResult(T v) {
        this.v = v;
    }

    public T getValue() {
        return v;
    }
}
