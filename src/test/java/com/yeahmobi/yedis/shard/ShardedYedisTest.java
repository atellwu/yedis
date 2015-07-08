package com.yeahmobi.yedis.shard;

import org.junit.Test;

public class ShardedYedisTest extends AbstractShardedYedisTest {

    @Test
    public void hashStrategyTest() {
        yedis.set("key1", "value1");
        yedis.set("key2", "value2");
        int size = groups.size();
        assertEquals("value1", groups.get(hash("key1", size)).get("key1"));
        assertEquals("value2", groups.get(hash("key2", size)).get("key2"));
        flushAll();
        groups.get(hash("key1", size)).set("key1", "value1");
        groups.get(hash("key2", size)).set("key2", "value2");
        assertEquals("value1", yedis.get("key1"));
        assertEquals("value2", yedis.get("key2"));
        flushAll();

    }

    private int hash(String str, int size) {
        return Math.abs(str.hashCode()) % size;
    }
}
