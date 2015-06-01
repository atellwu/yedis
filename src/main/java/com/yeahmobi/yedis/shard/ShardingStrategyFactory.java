package com.yeahmobi.yedis.shard;

import java.util.List;

import com.yeahmobi.yedis.group.GroupYedis;

public class ShardingStrategyFactory {

	private ShardingStrategyFactory() {

	}

	public static ShardingStrategy createShardingStrategy(
			List<GroupYedis> groups, ShardingAlgorithm algo,
			HashCodeComputingStrategy hashCodeComputingStrategy) {
		if (algo == ShardingAlgorithm.SIMPLE_HASH) {
			return new SimpleHashingShardingStrategy(groups,
					hashCodeComputingStrategy);
		}

		throw new IllegalArgumentException("Unsupported sharding algorithm.");
	}
}
