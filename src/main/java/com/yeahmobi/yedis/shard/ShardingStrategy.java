package com.yeahmobi.yedis.shard;

import com.yeahmobi.yedis.group.GroupYedis;

/**
 * @author Leo.Liang
 */
public interface ShardingStrategy {

    public GroupYedis route(String key);

    public GroupYedis route(byte[] key);

    public void flushAll();

    public void close();
}
