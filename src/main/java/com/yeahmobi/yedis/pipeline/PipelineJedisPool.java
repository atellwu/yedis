package com.yeahmobi.yedis.pipeline;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.yeahmobi.yedis.atomic.AtomConfig;

public class PipelineJedisPool {

    private final JedisPool  jedisPool;

    private final AtomConfig config;

    public PipelineJedisPool(AtomConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Argument cannot be null.");
        }

        this.config = config;
        this.jedisPool = new JedisPool(config.getPipelinePoolConfig(), config.getHost(), config.getPort(),
                                       config.getSocketTimeout(), config.getPassword(), config.getDatabase());
    }

    public AtomConfig getConfig() {
        return config;
    }

    public Jedis getJedis() {
        return this.jedisPool.getResource();
    }

    public void returnBrokenJedis(Jedis resource) {
        jedisPool.returnBrokenResource(resource);
    }

    public void returnJedis(Jedis resource) {
        jedisPool.returnResource(resource);
    }

    public void destroy() {
        jedisPool.destroy();
    }
    
    

}
