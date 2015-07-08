package com.yeahmobi.yedis.async;

import redis.clients.jedis.Jedis;


public class ThreadLocalHolder {

    public static final ThreadLocal<Jedis> jedisHolder = new ThreadLocal<Jedis>();
    

}
