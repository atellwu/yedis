package com.yeahmobi.yedis.group;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BitOP;

public class GroupYedisStringTest extends GroupYedisTest {

    private String key1 = "key1";
    private String key2 = "key2";

    @Before
    public void setup() {
        yedis.set(key1, "value1");
        yedis.set(key2, "value2");
    }

    @After
    public void tearDown() {
        yedis.flushAll();
    }

    @Test
    public void append() {
        yedis.append(key1, "_add");
        assertEquals("value1_add", yedis.get(key1));
    }

    @Test
    public void appendByte() {
        yedis.append(key1.getBytes(), "_add".getBytes());
        assertEquals("value1_add", yedis.get(key1));
    }

    @Test
    public void bitcount() {
        yedis.set(key1, "");
        yedis.setbit(key1, 0, true);
        assertEquals(Long.valueOf(1), yedis.bitcount(key1));
    }

    @Test
    public void bitcountByte() {
        yedis.set(key1, "");
        yedis.setbit(key1, 0, true);
        assertEquals(Long.valueOf(1), yedis.bitcount(key1.getBytes()));
    }

    @Test
    public void bitop() {
        yedis.setbit("key3", 0, true);
        yedis.setbit("key4", 1, true);
        yedis.bitop(BitOP.AND, "key5", "key3", "key4");
        assertFalse(yedis.getbit("key5", 1));
        yedis.bitop(BitOP.OR, "key5", "key3", "key4");
        assertTrue(yedis.getbit("key5", 1));
    }

    @Test
    public void bitopByte() {
        yedis.setbit("key3", 0, true);
        yedis.setbit("key4", 1, true);
        yedis.bitop(BitOP.AND, "key5".getBytes(), "key3".getBytes(), "key4".getBytes());
        assertFalse(yedis.getbit("key5", 1));
        yedis.bitop(BitOP.OR, "key5".getBytes(), "key3".getBytes(), "key4".getBytes());
        assertTrue(yedis.getbit("key5", 1));
    }

    @Test
    public void decr() {
        yedis.set(key1, "5");
        yedis.decr(key1);
        assertEquals("4", yedis.get(key1));
    }

    @Test
    public void decrByte() {
        yedis.set(key1, "5");
        yedis.decr(key1.getBytes());
        assertEquals("4", yedis.get(key1));
    }

    @Test
    public void decrBy() {
        yedis.set(key1, "5");
        yedis.decrBy(key1, 2);
        assertEquals("3", yedis.get(key1));
    }

    @Test
    public void decrByByte() {
        yedis.set(key1, "5");
        yedis.decrBy(key1.getBytes(), 2);
        assertEquals("3", yedis.get(key1));
    }

    @Test
    public void getrange() {
        assertEquals("value1", yedis.getrange(key1, 0, -1));
    }

    @Test
    public void getrangeByte() {
        assertEquals("value1", new String(yedis.getrange(key1.getBytes(), 0, -1)));
    }

    @Test
    public void getset() {
        assertEquals("value1", yedis.getSet(key1, "value"));
        assertEquals("value", yedis.get(key1));
    }

    @Test
    public void getsetByte() {
        assertEquals("value1", new String(yedis.getSet(key1.getBytes(), "value".getBytes())));
        assertEquals("value", yedis.get(key1));
    }

    @Test
    public void incr() {
        yedis.set(key1, "5");
        yedis.incr(key1);
        assertEquals("6", yedis.get(key1));
    }

    @Test
    public void incrByte() {
        yedis.set(key1, "5");
        yedis.incr(key1.getBytes());
        assertEquals("6", yedis.get(key1));
    }

    @Test
    public void incrBy() {
        yedis.set(key1, "5");
        yedis.incrBy(key1, 2);
        assertEquals("7", yedis.get(key1));
    }

    @Test
    public void incrByByte() {
        yedis.set(key1, "5");
        yedis.incrBy(key1.getBytes(), 2);
        assertEquals("7", yedis.get(key1));
    }

    @Test
    public void msetAndGet() {
        yedis.mset(key1, "5", key2, "6");
        List<String> list = yedis.mget(key1, key2);
        assertEquals("5", list.get(0));
        assertEquals("6", list.get(1));
    }

    @Test
    public void msetAndGetByte() {
        yedis.mset(key1, "5", key2, "6");
        List<byte[]> list = yedis.mget(key1.getBytes(), key2.getBytes());
        assertEquals("5", new String(list.get(0)));
        assertEquals("6", new String(list.get(1)));
    }

    @Test
    public void msetnx() {
        yedis.msetnx(key1, "5", key2, "6");
        assertEquals("value1", yedis.get(key1));
        assertEquals("value2", yedis.get(key2));
    }

    @Test
    public void msetnxByte() {
        yedis.msetnx(key1.getBytes(), "5".getBytes(), key2.getBytes(), "6".getBytes());
        assertEquals("value1", yedis.get(key1));
        assertEquals("value2", yedis.get(key2));
    }

    @Test
    public void psetex() {
        yedis.psetex(key1, 1000, "5");
        assertTrue(yedis.pttl(key1).compareTo(1000L) <= 0);
    }

    @Test
    public void psetexByte() {
        yedis.psetex(key1.getBytes(), 1000, "5".getBytes());
        assertTrue(yedis.pttl(key1).compareTo(1000L) <= 0);
    }

    @Test
    public void setex() {
        yedis.setex(key1, 10, "5");
        assertTrue(yedis.ttl(key1).compareTo(10L) <= 0);
    }

    @Test
    public void setexByte() {
        yedis.setex(key1.getBytes(), 10, "5".getBytes());
        assertTrue(yedis.ttl(key1).compareTo(10L) <= 0);
    }

    @Test
    public void setnx() {
        yedis.setnx(key1, "5");
        assertEquals("value1", yedis.get(key1));
    }

    @Test
    public void setnxByte() {
        yedis.setnx(key1.getBytes(), "5".getBytes());
        assertEquals("value1", yedis.get(key1));
    }

    @Test
    public void setrange() {
        yedis.setrange(key1, 0, "a");
        assertEquals("aalue1", yedis.get(key1));
    }

    @Test
    public void setrangeByte() {
        yedis.setrange(key1.getBytes(), 0, "a".getBytes());
        assertEquals("aalue1", yedis.get(key1));
    }

    @Test
    public void strlen() {
        assertEquals(Long.valueOf(6), yedis.strlen(key1));
    }

    @Test
    public void strlenByte() {
        assertEquals(Long.valueOf(6), yedis.strlen(key1.getBytes()));
    }

}
