package com.yeahmobi.yedis.loadbalance;

import com.yeahmobi.yedis.atomic.Yedis;

/**
 * @author atell
 */
public interface LoadBalancer {

    Yedis route();

    Type getType();
    
    public enum Type {
        RANDOM, ROUND_ROBIN
    }
}
