package com.yeahmobi.yedis.group;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GroupYedisKeyTest extends GroupYedisTest {

    private String           key1    = "key1";
    private String           key2    = "key2";

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
    public void delMulti() {
        assertEquals(Long.valueOf(2), yedis.del(key1, key2));
    }

    @Test
    public void delMultiByte() {
        assertEquals(Long.valueOf(2), yedis.del(key1.getBytes(), key2.getBytes()));
    }

    @Test
    public void dumpAndRestore() {
        byte[] dump = yedis.dump(key1);
        yedis.restore("key3", 0, dump);
        assertEquals("value1", yedis.get("key3"));
    }

    @Test
    public void dumpAndRestoreByte() {
        byte[] dump = yedis.dump(key1.getBytes());
        yedis.restore("key3".getBytes(), 0, dump);
        assertEquals("value1", yedis.get("key3"));
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
    public void keys() {
        assertEquals(2, yedis.keys("key*").size());
    }

    @Test
    public void keysByte() {
        assertEquals(2, yedis.keys("key*".getBytes()).size());
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

    @SuppressWarnings("deprecation")
    @Test
    public void pexpire() {
        yedis.pexpire(key1, 0);
        assertNull(yedis.get(key1));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void pexpireByte() {
        yedis.pexpire(key1.getBytes(), 0);
        assertNull(yedis.get(key1));
    }

    @Test
    public void pexpireAt() {
        yedis.pexpireAt(key1, 1);
        assertNull(yedis.get(key1));
    }

    @Test
    public void pexpireAtByte() {
        yedis.pexpireAt(key1.getBytes(), 1);
        assertNull(yedis.get(key1));
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
    public void pttl() {
        yedis.expire(key1, 10);
        yedis.persist(key1);
        assertEquals(Long.valueOf(-1), yedis.pttl(key1));
    }

    @Test
    public void pttlByte() {
        yedis.expire(key1, 10);
        yedis.persist(key1.getBytes());
        assertEquals(Long.valueOf(-1), yedis.pttl(key1));
    }

    @Test
    public void randomKey() {
        assertNotNull(yedis.randomKey());
    }

    @Test
    public void rename() {
        yedis.rename(key1, "key3");
        assertEquals("value1", yedis.get("key3"));
    }

    @Test
    public void renameByte() {
        yedis.rename(key1.getBytes(), "key3".getBytes());
        assertEquals("value1", yedis.get("key3"));
    }

    @Test
    public void renamenx() {
        yedis.renamenx(key1, "key2");
        assertEquals("value1", yedis.get(key1));
    }

    @Test
    public void renamenxByte() {
        yedis.renamenx(key1.getBytes(), "key2".getBytes());
        assertEquals("value1", yedis.get(key1));
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
