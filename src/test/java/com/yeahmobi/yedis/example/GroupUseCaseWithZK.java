package com.yeahmobi.yedis.example;

import java.util.concurrent.TimeUnit;

import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.group.ReadMode;

public class GroupUseCaseWithZK {

    public static void main(String[] args) throws InterruptedException {

        GroupConfig groupConfig = new GroupConfig("cluster_dsp","172.20.0.100");
        groupConfig.setReadMode(ReadMode.SLAVE);
        GroupYedis yedis = new GroupYedis(groupConfig);

        String key = "example_a";
        while (true) {
            try {
                yedis.set(key, "test");
                System.out.println(yedis.get(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.SECONDS.sleep(1);
        }

//        System.out.println("main done");
    }

}
