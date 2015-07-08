package com.yeahmobi.yedis.loadbalance;

import java.util.List;
import java.util.Random;

import com.yeahmobi.yedis.atomic.Yedis;

public class RandomLoadBalancer implements LoadBalancer {

    private Type         type   = LoadBalancer.Type.RANDOM;
    private List<Yedis>  yedisList;
    private int          listSize;

    private final Random random = new Random();

    public RandomLoadBalancer(List<Yedis> yedisList) {
        if (yedisList == null || yedisList.size() <= 0) {
            throw new IllegalArgumentException("yedisList cannot be null or empty.");
        }
        this.yedisList = yedisList;
        this.listSize = yedisList.size();
    }

    @Override
    public Yedis route() {
        return yedisList.get(random.nextInt(listSize));
    }

    @Override
    public Type getType() {
        return type;
    }

}
