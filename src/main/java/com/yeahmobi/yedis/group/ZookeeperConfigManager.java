package com.yeahmobi.yedis.group;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Verify;
import com.yeahmobi.yedis.common.ServerInfo;

/**
 * @author atell
 */
public class ZookeeperConfigManager implements MasterSlaveConfigManager, CuratorWatcher {

    private final static Logger        logger                        = LoggerFactory.getLogger(ZookeeperConfigManager.class);

    private static final int           DEFAULT_MAX_SLEEP_TIME        = 30000;
    private static final int           DEFAULT_BASE_SLEEP_TIME       = 500;

    private static final int           DEFAULT_SESSION_TIMEOUT_MS    = 60 * 1000;
    private static final int           DEFAULT_CONNECTION_TIMEOUT_MS = 15 * 1000;

    private static final String        NAMESPACE                     = "yedis/failover";

    private final CuratorFramework     client;

    private final String               clusterName;

    private List<ConfigChangeListener> listeners                     = new ArrayList<ConfigChangeListener>();

    private ServerInfo                 masterSeverInfo;

    private List<ServerInfo>           slaveSeverInfos;

    public ZookeeperConfigManager(String clusterName, String zkUrl, int sessionTimeoutMs, int connectionTimeoutMs,
                                  RetryPolicy retryPolicy) throws Exception {
        this.clusterName = clusterName;

        // 构造并启动zk client
        this.client = CuratorFrameworkFactory.builder().connectString(zkUrl).sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).namespace(NAMESPACE).retryPolicy(retryPolicy).build();
        client.start();

        // 获取并解析failoverStr，内容如：{"master":"172.20.0.53:6379","slaves":["172.20.0.55:6379","172.20.0.56:6379"],"unavailable":[]}
        byte[] data = client.getData().forPath(clusterName);
        Failover failover = parseFailoverStr(data);
        this.masterSeverInfo = parseServerInfo(failover.getMaster());
        this.slaveSeverInfos = parseSlaves(failover.getSlaves());

        // 设置zk监听
        client.getData().usingWatcher(this).inBackground().forPath(clusterName);

    }

    private Failover parseFailoverStr(byte[] data) throws Exception, UnsupportedEncodingException {
        String failoverStr = new String(data, "UTF-8");
        Failover failover = JSON.parseObject(failoverStr, Failover.class);
        logger.info("cluster config is:" + failoverStr);
        return failover;
    }

    public ZookeeperConfigManager(String clusterName, String zkUrl, int sessionTimeoutMs, int connectionTimeoutMs)
                                                                                                                  throws Exception {
        this(clusterName, zkUrl, sessionTimeoutMs, connectionTimeoutMs,
             new BoundedExponentialBackoffRetry(DEFAULT_BASE_SLEEP_TIME, DEFAULT_MAX_SLEEP_TIME, Integer.MAX_VALUE));
    }

    public ZookeeperConfigManager(String clusterName, String zkUrl, RetryPolicy retryPolicy) throws Exception {
        this(clusterName, zkUrl, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, retryPolicy);
    }

    public ZookeeperConfigManager(String clusterName, String zkUrl) throws Exception {
        this(clusterName, zkUrl, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
             new BoundedExponentialBackoffRetry(DEFAULT_BASE_SLEEP_TIME, DEFAULT_MAX_SLEEP_TIME, Integer.MAX_VALUE));
    }

    private List<ServerInfo> parseSlaves(List<String> slaves) {
        Verify.verify(slaves != null);

        List<ServerInfo> list = new ArrayList<ServerInfo>();
        if (slaves != null) {
            for (String str : slaves) {
                ServerInfo serverInfo = parseServerInfo(str);
                list.add(serverInfo);
            }
        }
        return list;
    }

    private ServerInfo parseServerInfo(String nodeStr) {
        try {
            Verify.verify(!Strings.isNullOrEmpty(nodeStr));

            Iterable<String> split = Splitter.on(':').split(nodeStr);
            Iterator<String> iterator = split.iterator();

            boolean hasNext = iterator.hasNext();
            Verify.verify(hasNext);

            String host = iterator.next();
            hasNext = iterator.hasNext();
            Verify.verify(hasNext);

            int port = Integer.parseInt(iterator.next());

            return new ServerInfo(host, port);

        } catch (Exception e) {
            throw new IllegalArgumentException("Error node string:" + nodeStr);
        }
    }

    @Override
    public ServerInfo getMasterServerInfo() {
        return this.masterSeverInfo;
    }

    @Override
    public List<ServerInfo> getSlaveServerInfos() {
        return this.slaveSeverInfos;
    }

    @Override
    public void addListener(ConfigChangeListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void close() {
        client.close();
    }

    public static class Failover {

        private String       master;
        private List<String> slaves;

        public String getMaster() {
            return master;
        }

        public void setMaster(String master) {
            this.master = master;
        }

        public List<String> getSlaves() {
            return slaves;
        }

        public void setSlaves(List<String> slaves) {
            this.slaves = slaves;
        }

    }

    @Override
    public void process(WatchedEvent event) throws Exception {
        logger.info("Config changed.");

        try {
            // 获取最新配置
            ServerInfo masterSeverInfo = null;
            List<ServerInfo> slaveSeverInfos = null;
            byte[] data = client.getData().forPath(clusterName);
            Failover failover = parseFailoverStr(data);
            masterSeverInfo = parseServerInfo(failover.getMaster());
            slaveSeverInfos = parseSlaves(failover.getSlaves());

            // 计算是否配置发生变更
            boolean changed = false;
            if (!this.masterSeverInfo.equals(masterSeverInfo)) {
                this.masterSeverInfo = masterSeverInfo;
                changed = true;
            }
            if (!equals(this.slaveSeverInfos, slaveSeverInfos)) {
                this.slaveSeverInfos = slaveSeverInfos;
                changed = true;
            }

            // 通知listener
            if (changed && listeners != null && listeners.size() > 0) {
                for (ConfigChangeListener listener : listeners) {
                    try {
                        listener.onChanged(this);
                    } catch (Exception e) {
                        logger.error("Config changed, but error occurred when invoke this listener.", e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Received watch event from zookeeper, but error occurred, so nothing changed.", e);
        } finally {
            // 再次监听
            client.getData().usingWatcher(this).inBackground().forPath(clusterName);
        }

    }

    private boolean equals(List<ServerInfo> list1, List<ServerInfo> list2) {
        if (list1 == null && list2 == null) {
            return true;
        } else if (list1 == null || list2 == null) {
            return false;
        } else if (list1.size() != list2.size()) {
            return false;
        }

        Comparator<? super ServerInfo> comparator = new Comparator<ServerInfo>() {

            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                String c1 = o1.getHost() + o1.getPort();
                String c2 = o2.getHost() + o2.getPort();
                return c1.compareTo(c2);
            }

        };
        Collections.sort(list1, comparator);
        Collections.sort(list2, comparator);

        return list1.equals(list2);
    }

    public String getClusterName() {
        return clusterName;
    }

}
