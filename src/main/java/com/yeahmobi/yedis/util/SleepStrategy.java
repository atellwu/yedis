package com.yeahmobi.yedis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 睡眠类
 * 
 * @author atell
 */
public class SleepStrategy {

    private static Logger logger = LoggerFactory.getLogger(SleepStrategy.class);

    private int           count  = 0;
    private final int     base;
    private final int     interval;
    private final int     upperbound;

    public SleepStrategy(int base, int interval, int upperbound) {
        super();
        this.base = base;
        this.interval = interval;
        this.upperbound = upperbound;
    }

    public SleepStrategy() {
        this(100, 500, 10000);
    }

    public long sleep() {
        long sleepTime = base + (long) count++ * interval;
        if (sleepTime > upperbound) {
            sleepTime = upperbound;
            count = 0;
        }
        if (sleepTime > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Sleep " + sleepTime + "ms.");
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return sleepTime;
    }

    public void reset() {
        count = 0;
    }

}
