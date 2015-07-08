package com.yeahmobi.yedis.group;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.yeahmobi.yedis.base.YedisTestBase;
import com.yeahmobi.yedis.common.ServerInfo;

public abstract class GroupYedisTest extends YedisTestBase {

    protected static GroupYedis yedis;

    @BeforeClass
    public static void initGroupYedis() throws IOException {
        ServerInfo writeSeverInfo = new ServerInfo(host, port);
        ServerInfo readSeverInfo = new ServerInfo(host, port);
        List<ServerInfo> readSeverInfoList = new ArrayList<ServerInfo>();
        readSeverInfoList.add(readSeverInfo);

        GroupConfig groupConfig = new GroupConfig(writeSeverInfo, readSeverInfoList);
        yedis = new GroupYedis(groupConfig);
    }
    
    @AfterClass
    public static void shutdown(){
    	yedis.close();
    }

}
