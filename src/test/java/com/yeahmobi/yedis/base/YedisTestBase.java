package com.yeahmobi.yedis.base;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import redis.embedded.RedisServer;

public abstract class YedisTestBase extends Assert {

    private static RedisServer redisServer;

    protected static String    host = "localhost";
//    protected static String    host = "172.20.0.100";

    protected static int       port = 63799;

    @BeforeClass
    public static void startRedisServer() throws IOException {
        redisServer = new RedisServer(port);
        redisServer.start();

    }

    @AfterClass
    public static void stopRedisServer() throws InterruptedException {
        // do some work
        redisServer.stop();
    }

}
