package com.yeahmobi.yedis.shard;

public class DefaultHashCodeCoputingStrategy implements HashCodeComputingStrategy{

    @Override
    public int hash(String key) {
        return key.hashCode();
    }

}
