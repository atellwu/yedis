package com.yeahmobi.yedis.base;

import java.io.IOException;

import org.junit.Assert;

import redis.embedded.RedisServer;

public abstract class DoubleServerYedisTestBase extends Assert {

    private static RedisServer redisServer1;
    private static RedisServer redisServer2;

    protected static String    host = "localhost";
    // protected static String host = "172.20.0.100";

    protected static int       port1 = 63800;
    protected static int       port2 = 63801;


    public static void startRedisServer() throws IOException {
        redisServer1 = new RedisServer(port1);
        redisServer1.start();
        redisServer2 = new RedisServer(port2);
        redisServer2.start();

    }

    
    public static void stopRedisServer() throws InterruptedException {
        // do some work
        redisServer1.stop();
        redisServer2.stop();
    }

}
