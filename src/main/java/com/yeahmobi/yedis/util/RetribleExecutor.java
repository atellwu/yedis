package com.yeahmobi.yedis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yeahmobi.yedis.common.YedisNetworkException;
import com.yeahmobi.yedis.common.YedisTimeoutException;

public final class RetribleExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RetribleExecutor.class);

    public static <T> T execute(RetriableOperator<T> opr, int retryCount) {
        for (;;) {
            try {
                return opr.execute();
            } catch (YedisNetworkException e) {
                if (retryCount-- <= 0) {
                    throw e;
                } else {
                    logger.warn("Error occur, message is:" + e.getMessage() + ", exception is " + e + ", retrying...");
                    continue;
                }
            } catch (YedisTimeoutException e) {
                if (retryCount-- <= 0) {
                    throw e;
                } else {
                    logger.warn("Error occur, message is:" + e.getMessage() + ", exception is " + e + ", retrying...");
                    continue;
                }
            }
        }
    }

    public static interface RetriableOperator<T> {

        T execute();
    }

}
