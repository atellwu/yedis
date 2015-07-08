package com.yeahmobi.yedis.group;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BinaryClient.LIST_POSITION;

public class GroupYedisListTest extends GroupYedisTest {

    private static final int TIMEOUT = 1000;
    private String           list1   = "list_1";
    private String           list2   = "list_2";
    private String           list3   = "list_3";

    @Before
    public void setup() {
        yedis.lpush(list1, "value5", "value4", "value3", "value2", "value1");
        yedis.lpush(list2, "value7", "value6", "value3", "value2", "value1");
        yedis.lpush(list3, "value9", "value8", "value3", "value2", "value1");
    }

    @After
    public void tearDown() {
        yedis.del(list1, list2, list3);
    }

    @Test
    public void blpop() {
        assertEquals("value1", yedis.blpop(TIMEOUT, list1).get(1));
    }

    @Test
    public void blpopByte() {
        assertEquals("value1", new String(yedis.blpop(TIMEOUT, list1.getBytes()).get(1)));
    }

    @Test
    public void brpop() {
        assertEquals("value5", yedis.brpop(TIMEOUT, list1).get(1));
    }

    @Test
    public void brpopByte() {
        assertEquals("value5", new String(yedis.brpop(TIMEOUT, list1.getBytes()).get(1)));
    }

    @Test
    public void brpoplpush() {
        assertEquals("value5", yedis.brpoplpush(list1, list2, TIMEOUT));
    }

    @Test
    public void brpoplpushByte() {
        assertEquals("value5", new String(yedis.brpoplpush(list1.getBytes(), list2.getBytes(), TIMEOUT)));
    }

    @Test
    public void lindex() {
        assertEquals("value1", yedis.lindex(list1, 0));
    }

    @Test
    public void lindexByte() {
        assertEquals("value1", new String(yedis.lindex(list1.getBytes(), 0)));
    }

    @Test
    public void llen() {
        assertEquals(Long.valueOf(5L), yedis.llen(list1));
    }

    @Test
    public void llenByte() {
        assertEquals(Long.valueOf(5L), yedis.llen(list1.getBytes()));
    }

    @Test
    public void linsert() {
        assertEquals(Long.valueOf(6L), yedis.linsert(list1, LIST_POSITION.AFTER, "value1", "value"));
    }

    @Test
    public void linsertByte() {
        assertEquals(Long.valueOf(6L),
                     yedis.linsert(list1.getBytes(), LIST_POSITION.AFTER, "value1".getBytes(), "value".getBytes()));
    }

    @Test
    public void lpop() {
        assertEquals("value1", yedis.lpop(list1));
    }

    @Test
    public void lpopByte() {
        assertEquals("value1", new String(yedis.lpop(list1.getBytes())));
    }

    @Test
    public void lpush() {
        assertEquals(Long.valueOf(6L), yedis.lpush(list1, "value"));
    }

    @Test
    public void lpushByte() {
        assertEquals(Long.valueOf(6L), yedis.lpush(list1.getBytes(), "value".getBytes()));
    }

    @Test
    public void lpushx() {
        assertEquals(Long.valueOf(0L), yedis.lpushx("list4", "value"));
    }

    @Test
    public void lpushxByte() {
        assertEquals(Long.valueOf(0L), yedis.lpushx("list4".getBytes(), "value".getBytes()));
    }

    @Test
    public void lrange() {
        List<String> lrange = yedis.lrange(list1, 0, -1);
        assertEquals(5, lrange.size());
    }

    @Test
    public void lrangeByte() {
        List<byte[]> lrange = yedis.lrange(list1.getBytes(), 0, -1);
        assertEquals(5, lrange.size());
    }

    @Test
    public void lrem() {
        assertEquals(Long.valueOf(1), yedis.lrem(list1, 1, "value1"));
    }

    @Test
    public void lremByte() {
        assertEquals(Long.valueOf(1), yedis.lrem(list1.getBytes(), 1, "value1".getBytes()));
    }

    @Test
    public void lset() {
        yedis.lset(list1, 1, "value");
        assertEquals("value", yedis.lindex(list1, 1));
    }

    @Test
    public void lsetByte() {
        yedis.lset(list1.getBytes(), 1, "value".getBytes());
        assertEquals("value", yedis.lindex(list1, 1));
    }

    @Test
    public void ltrim() {
        yedis.ltrim(list1, 0, 1);
        assertEquals(Long.valueOf(2), yedis.llen(list1));
    }

    @Test
    public void ltrimByte() {
        yedis.ltrim(list1.getBytes(), 0, 1);
        assertEquals(Long.valueOf(2), yedis.llen(list1));
    }

    @Test
    public void rpop() {
        assertEquals("value5", yedis.rpop(list1));
    }

    @Test
    public void rpopByte() {
        assertEquals("value5", new String(yedis.rpop(list1.getBytes())));
    }

    @Test
    public void rpoplpush() {
        assertEquals("value5", yedis.rpoplpush(list1, list2));
    }

    @Test
    public void rpoplpushByte() {
        assertEquals("value5", new String(yedis.rpoplpush(list1.getBytes(), list2.getBytes())));
    }

    @Test
    public void rpush() {
        assertEquals(Long.valueOf(6L), yedis.rpush(list1, "value"));
    }

    @Test
    public void rpushByte() {
        assertEquals(Long.valueOf(6L), yedis.rpush(list1.getBytes(), "value".getBytes()));
    }

    @Test
    public void rpushx() {
        assertEquals(Long.valueOf(0L), yedis.rpushx("list4", "value"));
    }

    @Test
    public void rpushxByte() {
        assertEquals(Long.valueOf(0L), yedis.rpushx("list4".getBytes(), "value".getBytes()));
    }

    @Test
    public void flushAll() {
        yedis.flushAll();
        assertEquals(false, yedis.exists(list1));
    }

}
