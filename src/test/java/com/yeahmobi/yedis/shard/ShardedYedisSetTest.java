package com.yeahmobi.yedis.shard;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShardedYedisSetTest extends AbstractShardedYedisTest {

    private String key1 = "key1";
    private String key2 = "key2";

    @Before
    public void setup() {
        yedis.sadd(key1, "value1", "value2");
        yedis.sadd(key2, "value3", "value4");
    }

    @After
    public void tearDown() {
        flushAll();
    }

    @Test
    public void sadd() {
        yedis.sadd(key1, "value3", "value4");
        assertEquals(Long.valueOf(4), yedis.scard(key1));
    }

    @Test
    public void saddByte() {
        yedis.sadd(key1.getBytes(), "value3".getBytes(), "value4".getBytes());
        assertEquals(Long.valueOf(4), yedis.scard(key1));
    }

}
