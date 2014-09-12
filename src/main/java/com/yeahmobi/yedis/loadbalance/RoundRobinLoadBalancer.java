package com.yeahmobi.yedis.loadbalance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.yeahmobi.yedis.atomic.Yedis;

public class RoundRobinLoadBalancer implements LoadBalancer {

    private final Type          type  = LoadBalancer.Type.ROUND_ROBIN;
    private final List<Yedis>   yedisList;
    private final int           listSize;

    private final AtomicInteger index = new AtomicInteger(-1);

    public RoundRobinLoadBalancer(List<Yedis> yedisList) {
        if (yedisList == null || yedisList.size() <= 0) {
            throw new IllegalArgumentException("yedisList cannot be null or empty.");
        }
        this.yedisList = yedisList;
        this.listSize = yedisList.size();
    }

    @Override
    public Yedis route() {
        return yedisList.get(getIndex());
    }

    private int getIndex() {
        return Math.abs(index.incrementAndGet() % listSize);
    }

    @Override
    public Type getType() {
        return type;
    }

}
