package com.yeahmobi.yedis.example;

import java.util.List;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.atomic.Yedis;
import com.yeahmobi.yedis.pipeline.YedisPipeline;

public class YedisCase {

    public static void main(String[] args) {
        AtomConfig config = new AtomConfig("127.0.0.1");
        config.setSocketTimeout(1000);
        final Yedis yedis = new Yedis(config);

        {
            Thread t = new Thread() {

                @Override
                public void run() {
                    while (true) {
                        YedisPipeline pipeline = yedis.pipelined();
                        pipeline.set("p_1", "p_1");
                        pipeline.set("p_2", "p_2");
                        pipeline.get("p_1");
                        pipeline.get("1");
                        List<Object> response = pipeline.syncAndReturnAll();
                        // 或pipeline.sync();
                        System.out.println(response);
                        // 能正常输出以下行，则没问题
                        // [OK, OK, p_1, 0]
                        // [OK, OK, p_1, 1]
                    }
                }

            };
            t.start();
        }

        // 使用多线程进行干扰
        {
            Thread t = new Thread() {

                @Override
                public void run() {
                    while (true) {
                        yedis.set("1", "1");
                        yedis.set("1", "0");
                        // System.out.println(yedis.get("p_1"));
                    }
                }

            };
            t.start();
        }

    }

}
