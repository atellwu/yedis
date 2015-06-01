package com.yeahmobi.yedis.group;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;

import redis.clients.jedis.JedisPoolConfig;

import com.yeahmobi.yedis.atomic.AtomConfig;
import com.yeahmobi.yedis.common.ServerInfo;
import com.yeahmobi.yedis.common.YedisException;
import com.yeahmobi.yedis.loadbalance.LoadBalancer;
import com.yeahmobi.yedis.loadbalance.LoadBalancer.Type;

public class GroupConfig {

    private static final int DEFAULT_MAX_POOL_SIZE = 20;

    private static final int DEFAULT_MIN_IDLE = 0;

    private static final int DEFAULT_MAX_IDLE = -1;//unlimited

    private static final long DEFAULT_MAX_WAIT_MILLIS = 1000;

    // 读库负载均衡
    private LoadBalancer.Type        loadBalancerType = Type.ROUND_ROBIN;

    private MasterSlaveConfigManager masterSlaveConfigManager;

    // 数据库
    private int                      database         = 0;

    // redis的密码
    private String                   password;

    // Jedis底层connection的timeout
    private int                      socketTimeout    = 100;

    // 控制所有Yedis操作的超时
    private long                     timeout          = 100;

    private int                      threadPoolSize   = 5;

    private String                   clientName;

    private ReadMode                 readMode         = ReadMode.SLAVEPREFERRED;

    private JedisPoolConfig pipelinePoolConfig = new JedisPoolConfig();
    {
        pipelinePoolConfig.setMaxTotal(DEFAULT_MAX_POOL_SIZE);
        pipelinePoolConfig.setMaxIdle(DEFAULT_MAX_IDLE);
        pipelinePoolConfig.setMinIdle(DEFAULT_MIN_IDLE);
        pipelinePoolConfig.setMaxWaitMillis(DEFAULT_MAX_WAIT_MILLIS);
    }

    public GroupConfig(ServerInfo writeSeverInfo, List<ServerInfo> readSeverInfoList) {
        this.masterSlaveConfigManager = new DefaultConfigManager(writeSeverInfo, readSeverInfoList);
    }

    /**
     * 通过zookeeper获取 Master/Slave的动态配置
     * 
     * @param clusterName 集群名称，对应的zookeeper的路径是 /yedis/failover/[clusterName]
     * @param zkUrl zookeeper的连接字符串
     */
    public GroupConfig(String clusterName, String zkUrl) {
        try {
            this.masterSlaveConfigManager = new ZookeeperConfigManager(clusterName, zkUrl);
        } catch (Exception e) {
            throw new YedisException(e.getMessage(), e);
        }
    }

    /**
     * 通过zookeeper获取 Master/Slave的动态配置
     * 
     * @param rootpath zookeeper的根路径
     * @param clusterName 集群名称，对应的zookeeper的路径是 [rootpath]/[clusterName]
     * @param zkUrl zookeeper的连接字符串
     */
    public GroupConfig(String rootPath, String clusterName, String zkUrl) {
        try {
            this.masterSlaveConfigManager = new ZookeeperConfigManager(rootPath, clusterName, zkUrl);
        } catch (Exception e) {
            throw new YedisException(e.getMessage(), e);
        }
    }

    /**
     * 通过zookeeper获取 Master/Slave的动态配置
     * 
     * @param rootpath zookeeper的根路径
     * @param clusterName 集群名称，对应的zookeeper的路径是 [rootpath]/[clusterName]
     * @param zkUrl zookeeper的连接字符串
     * @param sessionTimeoutMs zookeeper的会话超时
     * @param connectionTimeoutMs zookeeper的连接超时
     * @param retryPolicy zookeeper的重试策略
     */
    public GroupConfig(String rootPath, String clusterName, String zkUrl, int sessionTimeoutMs,
                       int connectionTimeoutMs, RetryPolicy retryPolicy) {
        try {
            this.masterSlaveConfigManager = new ZookeeperConfigManager(rootPath, clusterName, zkUrl, sessionTimeoutMs,
                                                                       connectionTimeoutMs, retryPolicy);
        } catch (Exception e) {
            throw new YedisException(e.getMessage(), e);
        }
    }

    public ServerInfo getMasterServerInfo() {
        return masterSlaveConfigManager.getMasterServerInfo();
    }

    public List<ServerInfo> getSlaveServerInfoList() {
        return masterSlaveConfigManager.getSlaveServerInfos();
    }

    public void addListener(ConfigChangeListener listener) {
        masterSlaveConfigManager.addListener(listener);
    }

    public LoadBalancer.Type getLoadBalancerType() {
        return loadBalancerType;
    }

    public void setLoadBalancerType(LoadBalancer.Type loadBalancerType) {
        this.loadBalancerType = loadBalancerType;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public MasterSlaveConfigManager getMasterSlaveConfigManager() {
        return masterSlaveConfigManager;
    }

    public void setMasterSlaveConfigManager(MasterSlaveConfigManager masterSlaveConfigManager) {
        this.masterSlaveConfigManager = masterSlaveConfigManager;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public ReadMode getReadMode() {
        return readMode;
    }

    public void setReadMode(ReadMode readMode) {
        this.readMode = readMode;
    }

    public List<AtomConfig> getSlaveAtomConfigs() {
        List<ServerInfo> serverInfos = this.getSlaveServerInfoList();
        if (serverInfos != null) {
            List<AtomConfig> atomConfigs = new ArrayList<AtomConfig>(serverInfos.size());
            for (ServerInfo serverInfo : serverInfos) {
                AtomConfig atomConfig = new AtomConfig();
                atomConfig.setClientName(clientName);
                atomConfig.setDatabase(database);
                atomConfig.setPassword(password);
                atomConfig.setServerInfo(serverInfo);
                atomConfig.setSocketTimeout(socketTimeout);
                atomConfig.setThreadPoolSize(threadPoolSize);
                atomConfig.setTimeout(timeout);
                atomConfig.setPipelinePoolConfig(pipelinePoolConfig);
                atomConfigs.add(atomConfig);
            }
            return atomConfigs;
        }
        return null;

    }

    public AtomConfig getMasterAtomConfig() {
        ServerInfo serverInfo = this.getMasterServerInfo();
        if (serverInfo != null) {
            AtomConfig atomConfig = new AtomConfig();
            atomConfig.setClientName(clientName);
            atomConfig.setDatabase(database);
            atomConfig.setPassword(password);
            atomConfig.setServerInfo(serverInfo);
            atomConfig.setSocketTimeout(socketTimeout);
            atomConfig.setThreadPoolSize(threadPoolSize);
            atomConfig.setTimeout(timeout);
            atomConfig.setPipelinePoolConfig(pipelinePoolConfig);
            return atomConfig;
        }
        return null;
    }

    public JedisPoolConfig getPipelinePoolConfig() {
        return pipelinePoolConfig;
    }

    @Override
    public String toString() {
        return "GroupConfig [loadBalancerType=" + loadBalancerType + ", masterSlaveConfigManager="
               + masterSlaveConfigManager + ", database=" + database + ", password=" + password + ", socketTimeout="
               + socketTimeout + ", timeout=" + timeout + ", threadPoolSize=" + threadPoolSize + ", clientName="
               + clientName + ", readMode=" + readMode + ", pipelinePoolConfig=" + pipelinePoolConfig + "]";
    }

}
