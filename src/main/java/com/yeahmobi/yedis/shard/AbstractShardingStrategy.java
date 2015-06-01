package com.yeahmobi.yedis.shard;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.yeahmobi.yedis.common.YedisException;
import com.yeahmobi.yedis.group.GroupYedis;

public abstract class AbstractShardingStrategy implements ShardingStrategy {
	protected HashCodeComputingStrategy hashCodeComputingStrategy;
	protected static final String STRING_ENCODING = "utf-8";

	protected AbstractShardingStrategy(List<GroupYedis> groups,
			HashCodeComputingStrategy hashCodeComputingStrategy) {
		if (groups == null || groups.isEmpty()) {
			throw new IllegalArgumentException(
					"You must init with at least one GroupYedis instance.");
		}

		if (hashCodeComputingStrategy == null) {
			throw new IllegalArgumentException(
					"You must init with an not null hashCodeComputingStrategy.");
		}

		this.hashCodeComputingStrategy = hashCodeComputingStrategy;
	}

	protected String bytesToArray(byte[] data) {
		try {
			return new String(data, STRING_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new YedisException(e);
		}
	}

}
