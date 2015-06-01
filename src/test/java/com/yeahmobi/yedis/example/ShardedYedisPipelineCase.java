package com.yeahmobi.yedis.example;

import java.util.ArrayList;
import java.util.List;

import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.pipeline.ShardedYedisPipeline;
import com.yeahmobi.yedis.shard.DefaultHashCodeCoputingStrategy;
import com.yeahmobi.yedis.shard.ShardedYedis;
import com.yeahmobi.yedis.shard.ShardingAlgorithm;

public class ShardedYedisPipelineCase {

    protected static String host = "localhost";

    private static GroupYedis buildGroupYedis(int port) {
        ServerInfo writeSeverInfo = new ServerInfo(host, port);
        ServerInfo readSeverInfo = new ServerInfo(host, port);
        List<ServerInfo> readSeverInfoList = new ArrayList<ServerInfo>();
        readSeverInfoList.add(readSeverInfo);

        GroupConfig groupConfig = new GroupConfig(writeSeverInfo, readSeverInfoList);
        groupConfig.getPipelinePoolConfig().setMaxTotal(1);
        GroupYedis groupYedis = new GroupYedis(groupConfig);
        return groupYedis;
    }

    public static void main(String[] args) {
        GroupYedis groupYedis1 = buildGroupYedis(6379);
        GroupYedis groupYedis2 = buildGroupYedis(6380);
        ArrayList<GroupYedis> groups = new ArrayList<GroupYedis>();
        groups.add(groupYedis1);
        groups.add(groupYedis2);
        ShardedYedis yedis = new ShardedYedis(groups, ShardingAlgorithm.SIMPLE_HASH, new DefaultHashCodeCoputingStrategy());
        
        ShardedYedisPipeline pipeline = yedis.pipeline();
        pipeline.set("key1", "shard-value1");
        pipeline.set("key2", "shard-value2");
        pipeline.set("key3", "shard-value3");
        pipeline.get("key3");
        pipeline.get("key2");
        pipeline.get("key1");
        pipeline.get("key1");
        pipeline.get("key2");
        pipeline.get("key3");
        System.out.println(pipeline.syncAndReturnAll());
        pipeline.sync();
//        System.out.println(pipeline.sync());
        
        ShardedYedisPipeline pipeline2 = yedis.pipeline();
        pipeline2.get("key1");
        pipeline2.get("key2");
        
        System.out.println(pipeline2.syncAndReturnAll());
    }

}
