package com.yeahmobi.yedis.shard;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShardedYedisKeyTest extends AbstractShardedYedisTest {

    private String key1 = "key1";
    private String key2 = "key2";

    @Before
    public void setup() {
        yedis.set(key1, "value1");
        yedis.set(key2, "value2");
    }

    @After
    public void tearDown() {
        flushAll();
    }

    @Test
    public void del() {
        assertEquals(Long.valueOf(1), yedis.del(key1));
        assertEquals(Long.valueOf(0), yedis.del("key3"));
    }

    @Test
    public void delByte() {
        assertEquals(Long.valueOf(1), yedis.del(key1.getBytes()));
        assertEquals(Long.valueOf(0), yedis.del("key3".getBytes()));
    }

    @Test
    public void exists() {
        assertEquals(true, yedis.exists(key1));
        assertEquals(false, yedis.exists("key3"));
    }

    @Test
    public void existsByte() {
        assertEquals(true, yedis.exists(key1.getBytes()));
        assertEquals(false, yedis.exists("key3".getBytes()));
    }

    @Test
    public void expire() {
        yedis.expire(key1, 0);
        assertNull(yedis.get(key1));
    }

    @Test
    public void expireByte() {
        yedis.expire(key1.getBytes(), 0);
        assertNull(yedis.get(key1));
    }

    @Test
    public void expireAt() {
        yedis.expireAt(key1, 1);
        assertNull(yedis.get(key1));
    }

    @Test
    public void expireAtByte() {
        yedis.expireAt(key1.getBytes(), 1);
        assertNull(yedis.get(key1));
    }

    @Test
    public void move() {
        yedis.move(key1, 1);
        assertNull(yedis.get(key1));
    }

    @Test
    public void moveByte() {
        yedis.move(key1.getBytes(), 1);
        assertNull(yedis.get(key1));
    }

    @Test
    public void persist() {
        yedis.expire(key1, 10);
        yedis.persist(key1);
        assertEquals(Long.valueOf(-1), yedis.ttl(key1));
    }

    @Test
    public void persistByte() {
        yedis.expire(key1, 10);
        yedis.persist(key1.getBytes());
        assertEquals(Long.valueOf(-1), yedis.ttl(key1));
    }

    @Test
    public void ttl() {
        yedis.expire(key1, 10);
        yedis.persist(key1);
        assertEquals(Long.valueOf(-1), yedis.ttl(key1));
    }

    @Test
    public void ttlByte() {
        yedis.expire(key1, 10);
        yedis.persist(key1.getBytes());
        assertEquals(Long.valueOf(-1), yedis.ttl(key1));
    }

    @Test
    public void sort() {
        yedis.lpush("key3", "1", "2");
        List<String> sortList = yedis.sort("key3");
        assertEquals("1", sortList.get(0));
    }

    @Test
    public void sortByte() {
        yedis.lpush("key3", "1", "2");
        List<byte[]> sortList = yedis.sort("key3".getBytes());
        assertEquals("1", new String(sortList.get(0)));
    }

    @Test
    public void type() {
        assertEquals("string", yedis.type(key1));
    }

    @Test
    public void typeByte() {
        assertEquals("string", yedis.type(key1.getBytes()));
    }

}
