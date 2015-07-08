package com.yeahmobi.yedis.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.pipeline.YedisPipeline;

public class GroupUseCase {

    public static void main(String[] args) throws InterruptedException {

        ServerInfo writeSeverInfo = new ServerInfo("127.0.0.1", 6379);
        ServerInfo readSeverInfo = new ServerInfo("127.0.0.1", 6379);
        List<ServerInfo> readSeverInfoList = new ArrayList<ServerInfo>();
        readSeverInfoList.add(readSeverInfo);

        GroupConfig groupConfig = new GroupConfig(writeSeverInfo, readSeverInfoList);
        GroupYedis yedis = new GroupYedis(groupConfig);

        String key = "example_a";
        while (true) {
            try {
                yedis.set(key, "test");
                YedisPipeline pipeline = yedis.pipelined();
                pipeline.set("p_1", "p_1");
                pipeline.set("p_2", "p_2");
                pipeline.get("p_1");
                pipeline.get("1");
                List<Object> response = pipeline.syncAndReturnAll();
                // 或pipeline.sync();
                System.out.println(response);
                System.out.println(yedis.get(key));
                yedis.del(key);
                System.out.println(yedis.get(key));
                System.out.println(yedis.get("p_2"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(5);
        }

//        System.out.println("main done");
    }

}
