package com.yeahmobi.yedis.group;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GroupYedisHashTest extends GroupYedisTest {

    private String key1  = "key1";
    private String key2  = "key2";
    private String key11 = "key11";
    private String key12 = "key12";
    private String key21 = "key21";
    private String key22 = "key22";

    @Before
    public void setup() {
        yedis.hset(key1, key11, "value11");
        yedis.hset(key1, key12, "value12");
        yedis.hset(key2, key21, "value21");
        yedis.hset(key2, key22, "value22");
    }

    @After
    public void tearDown() {
        yedis.flushAll();
    }

    @Test
    public void hgetAndDel() {
        assertEquals(Long.valueOf(1), yedis.hdel(key1, key11));
        assertEquals(null, yedis.hget(key1, key11));
    }

    @Test
    public void hgetAndDelByte() {
        assertEquals(Long.valueOf(1), yedis.hdel(key1.getBytes(), key11.getBytes()));
        assertEquals(null, yedis.hget(key1.getBytes(), key11.getBytes()));
    }

    @Test
    public void hexists() {
        assertTrue(yedis.hexists(key1, key11));
        assertFalse(yedis.hexists(key1, "nokey"));
    }

    @Test
    public void hexistsByte() {
        assertTrue(yedis.hexists(key1.getBytes(), key11.getBytes()));
        assertFalse(yedis.hexists(key1.getBytes(), "nokey".getBytes()));
    }

    @Test
    public void hgetall() {
        Map<String, String> map = yedis.hgetAll(key1);
        assertEquals(2, map.size());
        assertEquals("value11", map.get(key11));
    }

    @Test
    public void hgetallByte() {
        Map<byte[], byte[]> map = yedis.hgetAll(key1.getBytes());
        assertEquals(2, map.size());
        assertEquals("value11", new String(map.get(key11.getBytes())));
    }

    @Test
    public void hincby() {
        yedis.hset(key1, key11, "5");
        yedis.hincrBy(key1, key11, 2);
        assertEquals("7", yedis.hget(key1, key11));
    }

    @Test
    public void hincbyByte() {
        yedis.hset(key1, key11, "5");
        yedis.hincrBy(key1.getBytes(), key11.getBytes(), 2);
        assertEquals("7", yedis.hget(key1, key11));
    }

    @Test
    public void hincbyFloat() {
        yedis.hset(key1, key11, "5");
        yedis.hincrByFloat(key1, key11, 2);
        assertEquals("7", yedis.hget(key1, key11));
    }

    @Test
    public void hincbyFloatByte() {
        yedis.hset(key1, key11, "5");
        yedis.hincrByFloat(key1.getBytes(), key11.getBytes(), 2);
        assertEquals("7", yedis.hget(key1, key11));
    }

    @Test
    public void hkeys() {
        Set<String> hkeys = yedis.hkeys(key1);
        assertEquals(2, hkeys.size());
    }

    @Test
    public void hkeysByte() {
        Set<byte[]> hkeys = yedis.hkeys(key1.getBytes());
        assertEquals(2, hkeys.size());
    }

    @Test
    public void hlen() {
        Long len = yedis.hlen(key1);
        assertEquals(2, len.longValue());
    }

    @Test
    public void hlenByte() {
        Long len = yedis.hlen(key1.getBytes());
        assertEquals(2, len.longValue());
    }

    @Test
    public void hmget() {
        List<String> list = yedis.hmget(key1, key11, key12);
        assertEquals(2, list.size());
        assertEquals("value11", list.get(0));
        assertEquals("value12", list.get(1));
    }

    @Test
    public void hmgetByte() {
        List<byte[]> list = yedis.hmget(key1.getBytes(), key11.getBytes(), key12.getBytes());
        assertEquals(2, list.size());
        assertEquals("value11", new String(list.get(0)));
        assertEquals("value12", new String(list.get(1)));
    }

    @Test
    public void hmset() {
        Map<String, String> map = ImmutableMap.of(key11, "value", key12, "value");
        yedis.hmset(key1, map);
        assertEquals("value", yedis.hget(key1, key11));
        assertEquals("value", yedis.hget(key1, key12));
    }

    @Test
    public void hmsetByte() {
        Map<byte[], byte[]> map = ImmutableMap.of(key11.getBytes(), "value".getBytes(), key12.getBytes(),
                                                  "value".getBytes());
        yedis.hmset(key1.getBytes(), map);
        assertEquals("value", new String(yedis.hget(key1, key11)));
        assertEquals("value", new String(yedis.hget(key1, key12)));
    }

    @Test
    public void hsetnx() {
        yedis.hsetnx(key1, key11, "value");
        assertEquals("value11", yedis.hget(key1, key11));
    }

    @Test
    public void hsetnxByte() {
        yedis.hsetnx(key1.getBytes(), key11.getBytes(), "value".getBytes());
        assertEquals("value11", yedis.hget(key1, key11));
    }

    @Test
    public void hvals() {
        List<String> list = yedis.hvals(key1);
        assertEquals(2, list.size());
    }

    @Test
    public void hvalsByte() {
        List<byte[]> list = yedis.hvals(key1.getBytes());
        assertEquals(2, list.size());
    }
}
