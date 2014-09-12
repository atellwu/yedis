package com.yeahmobi.yedis.loadbalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.atomic.Yedis;

public class RandomLoadBalancerTest extends Assert {

    private static final String host      = "";

    private List<Yedis>         yedisList = new ArrayList<Yedis>();

    private Yedis               yedis1;
    private Yedis               yedis2;

    {
        AtomConfig config1 = new AtomConfig(host);
        yedis1 = new Yedis(config1);
        AtomConfig config2 = new AtomConfig(host);
        yedis2 = new Yedis(config2);
        yedisList.add(yedis1);
        yedisList.add(yedis2);

    }
    private RandomLoadBalancer  lb        = new RandomLoadBalancer(yedisList);

    @Test
    public void testType() {
        assertEquals(LoadBalancer.Type.RANDOM, lb.getType());
    }

    @Test
    public void testRoute() {
        Yedis yedis = lb.route();
        assertTrue(yedis == yedis1 || yedis == yedis2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testException() {
        new RandomLoadBalancer(null);
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IllegalArgumentException.class)
    public void testException2() {
        new RandomLoadBalancer(Collections.EMPTY_LIST);
    }

}
