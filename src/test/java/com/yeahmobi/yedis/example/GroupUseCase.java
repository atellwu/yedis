package com.yeahmobi.yedis.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;

public class GroupUseCase {

    public static void main(String[] args) throws InterruptedException {

        ServerInfo writeSeverInfo = new ServerInfo("172.20.0.100", 6379);
        ServerInfo readSeverInfo = new ServerInfo("172.20.0.101", 6379);
        List<ServerInfo> readSeverInfoList = new ArrayList<ServerInfo>();
        readSeverInfoList.add(readSeverInfo);

        GroupConfig groupConfig = new GroupConfig(writeSeverInfo, readSeverInfoList);
        GroupYedis yedis = new GroupYedis(groupConfig);

        String key = "example_a";
        while (true) {
            try {
                yedis.set(key, "test");
                System.out.println(yedis.get(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(5);
        }

//        System.out.println("main done");
    }

}
