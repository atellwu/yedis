package com.yeahmobi.yedis.shard;

import java.util.List;

import com.yeahmobi.yedis.group.GroupYedis;

/**
 * @author Leo.Liang
 */
public class DummyShardingStrategy extends AbstractShardingStrategy {

    private List<GroupYedis> groups;

    public DummyShardingStrategy(List<GroupYedis> groups, HashCodeComputingStrategy hashCodeComputingStrategy) {
        super(groups, hashCodeComputingStrategy);
        this.groups = groups;
    }

    @Override
    public GroupYedis route(String key) {
        return groups.get(0);
    }

    @Override
    public GroupYedis route(byte[] key) {
        return groups.get(0);
    }

    @Override
    public void close() {
        for (GroupYedis group : groups) {
            try {
                group.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void flushAll() {
        for (GroupYedis group : groups) {
            try {
                group.flushAll();
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
