package com.yeahmobi.yedis.shard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.yeahmobi.yedis.base.DoubleServerYedisTestBase;
import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;

public abstract class AbstractShardedYedisTest extends DoubleServerYedisTestBase {

    protected static ShardedYedis     yedis;

    protected static List<GroupYedis> groups;

    @BeforeClass
    public static void initGroupYedis() throws IOException {
        startRedisServer();
        GroupYedis groupYedis1 = buildGroupYedis(port1);
        GroupYedis groupYedis2 = buildGroupYedis(port2);
        groups = new ArrayList<GroupYedis>();
        groups.add(groupYedis1);
        groups.add(groupYedis2);
        yedis = new ShardedYedis(groups, ShardingAlgorithm.SIMPLE_HASH, new DefaultHashCodeCoputingStrategy());
    }
    
    @AfterClass
    public static void shutdown() throws InterruptedException {
        yedis.close();
        stopRedisServer();
    }

    private static GroupYedis buildGroupYedis(int port) {
        ServerInfo writeSeverInfo = new ServerInfo(host, port);
        ServerInfo readSeverInfo = new ServerInfo(host, port);
        List<ServerInfo> readSeverInfoList = new ArrayList<ServerInfo>();
        readSeverInfoList.add(readSeverInfo);

        GroupConfig groupConfig = new GroupConfig(writeSeverInfo, readSeverInfoList);
        GroupYedis groupYedis = new GroupYedis(groupConfig);
        return groupYedis;
    }

    void flushAll() {
        for(GroupYedis groupYedis:groups){
            groupYedis.flushAll();
        }
    }

   
}
