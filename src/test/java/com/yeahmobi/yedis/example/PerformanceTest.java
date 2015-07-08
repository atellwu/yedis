package com.yeahmobi.yedis.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.atomic.Yedis;
import com.yeahmobi.yedis.group.GroupConfig;
import com.yeahmobi.yedis.group.GroupYedis;
import com.yeahmobi.yedis.group.ReadMode;
import com.yeahmobi.yedis.util.RetribleExecutor;
import com.yeahmobi.yedis.util.RetribleExecutor.RetriableOperator;

public class PerformanceTest {

    private final static Logger logger = LoggerFactory.getLogger(GroupYedis.class);

    public static void main(String[] args) throws InterruptedException {
        try {
            {
                final GroupYedis yedis = getGroupYedis();
                Thread.sleep(3000);// 等待连接池构建完
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100000; i++) {
                    RetribleExecutor.execute(new RetriableOperator<String>() {

                        public String execute() {
                            return yedis.get("test_a");
                        }
                    }, Integer.MAX_VALUE);
                }
                System.out.println(System.currentTimeMillis() - start);
                yedis.close();
            }
            System.out.println("---------------------------------------------");
            {
                Yedis yedis = getAtomYedis();
                Thread.sleep(1000);// 等待连接池构建完
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10000; i++) {
                    yedis.get("test_a");
                }
                System.out.println(System.currentTimeMillis() - start);
                yedis.close();
            }
            System.out.println("---------------------------------------------");
            {
                Jedis jedis = getJedis();
                long start = System.currentTimeMillis();
                for (int i = 0; i < 10000; i++) {
                    // jedis.set("test_a", "test_a");
                    jedis.get("test_a");
                }
                System.out.println(System.currentTimeMillis() - start);
                jedis.close();
            }
        } catch (Exception e) {
            System.out.println("-----");
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
    }

    protected static Jedis getJedis() {
        Jedis jedis = new Jedis("172.20.0.100");
        return jedis;
    }

    protected static Yedis getAtomYedis() {
        AtomConfig config = new AtomConfig("172.20.0.100");
        Yedis yedis = new Yedis(config);
        return yedis;
    }

    private static GroupYedis getGroupYedis() {
        GroupConfig groupConfig = new GroupConfig("cluster_dsp", "172.20.0.100");
        groupConfig.setReadMode(ReadMode.SLAVE);
        GroupYedis yedis = new GroupYedis(groupConfig);
        return yedis;
    }

}
