package com.yeahmobi.yedis.async;

import java.util.concurrent.Callable;

import redis.clients.jedis.Jedis;

public abstract class AsyncOperation<T> implements Callable<OperationResult<T>> {

    public abstract OperationResult<T> execute(Jedis jedis);

    @Override
    public OperationResult<T> call() throws Exception {
        Jedis jedis = ThreadLocalHolder.jedisHolder.get();
        return execute(jedis);
    }

}
