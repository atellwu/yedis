package com.yeahmobi.yedis.group;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import redis.embedded.RedisServer;

public abstract class YedisBaseTest extends Assert {

    private static RedisServer redisServer;

    protected static String    host = "localhost";
//    protected static String    host = "172.20.0.100";

    protected static int       port = 6379;

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
