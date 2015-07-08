package com.yeahmobi.yedis.shard;

public interface HashCodeComputingStrategy {
	int hash(String key);
}
