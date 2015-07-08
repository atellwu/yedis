package com.yeahmobi.yedis.util;

import org.junit.Assert;
import org.junit.Test;

import com.yeahmobi.yedis.common.YedisNetworkException;
import com.yeahmobi.yedis.common.YedisTimeoutException;
import com.yeahmobi.yedis.util.RetribleExecutor.RetriableOperator;

public class RetribleExecutorTest extends Assert {

    @Test
    public void test1() {
        int retryCount = 3;
        RetribleExecutor.execute(new RetriableOperator<String>() {

            int i = 0;

            public String execute() {
                if (i == 0) {
                    i++;
                    throw new YedisTimeoutException();
                } else if (i == 1) {
                    i++;
                    throw new YedisNetworkException();
                }
                return "done";
            }
        }, retryCount);
    }

    @Test(expected = YedisNetworkException.class)
    public void testException1() {
        int retryCount = 1;
        RetribleExecutor.execute(new RetriableOperator<String>() {

            int i = 0;

            public String execute() {
                if (i == 0) {
                    i++;
                    throw new YedisTimeoutException();
                } else if (i == 1) {
                    i++;
                    throw new YedisNetworkException();
                }
                return "done";
            }
        }, retryCount);
    }

    @Test(expected = YedisTimeoutException.class)
    public void testException2() {
        int retryCount = 0;
        RetribleExecutor.execute(new RetriableOperator<String>() {

            int i = 0;

            public String execute() {
                if (i == 0) {
                    i++;
                    throw new YedisTimeoutException();
                } else if (i == 1) {
                    i++;
                    throw new YedisNetworkException();
                }
                return "done";
            }
        }, retryCount);
    }

}
