package com.yeahmobi.yedis.loadbalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.atomic.Yedis;
import com.yeahmobi.yedis.base.YedisBaseTest;

public class RoundRobinLoadBalancerTest extends YedisBaseTest {

	private static List<Yedis> yedisList = new ArrayList<Yedis>();

	private static Yedis yedis1;
	private static Yedis yedis2;

	@BeforeClass
	public static void init() {
		AtomConfig config1 = new AtomConfig(host, port);
		yedis1 = new Yedis(config1);
		AtomConfig config2 = new AtomConfig(host, port);
		yedis2 = new Yedis(config2);
		yedisList.add(yedis1);
		yedisList.add(yedis2);

	}

	private RoundRobinLoadBalancer lb = new RoundRobinLoadBalancer(yedisList);

	@Test
	public void testType() {
		assertEquals(LoadBalancer.Type.ROUND_ROBIN, lb.getType());
	}

	@Test
	public void testRoute() {
		Yedis yedis = lb.route();
		assertTrue(yedis == yedis1 || yedis == yedis2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testException() {
		new RoundRobinLoadBalancer(null);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = IllegalArgumentException.class)
	public void testException2() {
		new RoundRobinLoadBalancer(Collections.EMPTY_LIST);
	}

}
