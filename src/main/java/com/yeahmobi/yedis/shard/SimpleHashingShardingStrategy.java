package com.yeahmobi.yedis.shard;

import java.util.ArrayList;
import java.util.List;

import com.yeahmobi.yedis.group.GroupYedis;

/**
 * @author Leo.Liang
 */
public class SimpleHashingShardingStrategy extends AbstractShardingStrategy {

    private ArrayList<GroupYedis> groups;
    private int                   size = 0;

    public SimpleHashingShardingStrategy(List<GroupYedis> groups, HashCodeComputingStrategy hashCodeComputingStrategy) {
        super(groups, hashCodeComputingStrategy);
        // For the sake of random accessing performance,
        // we should ensure the groups is an array based list.
        this.groups = new ArrayList<GroupYedis>(groups);
        this.size = this.groups.size();
    }

    @Override
    public GroupYedis route(String key) {
        int hash = Math.abs(this.hashCodeComputingStrategy.hash(key));
        return this.groups.get(hash % size);
    }

    @Override
    public GroupYedis route(byte[] key) {
        return this.groups.get(this.hashCodeComputingStrategy.hash(bytesToArray(key)) % size);
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
