package com.yeahmobi.yedis.atomic;

import redis.clients.jedis.JedisPoolConfig;

import com.yeahmobi.yedis.common.ServerInfo;

public class AtomConfig implements Cloneable {

    private static final int DEFAULT_PORT   = 6379;

    private static final int DEFAULT_MAX_POOL_SIZE = 20;

    private static final int DEFAULT_MIN_IDLE = 0;

    private static final int DEFAULT_MAX_IDLE = -1;//unlimited

    private static final long DEFAULT_MAX_WAIT_MILLIS = 1000;

    // host和port
    private ServerInfo       serverInfo;

    // 数据库
    private int              database       = 0;

    // redis的密码
    private String           password;

    // Jedis底层connection的timeout
    private int              socketTimeout  = 100;

    // 控制所有Yedis操作的超时
    private long             timeout        = 100;

    private int              threadPoolSize = 5;

    private String           clientName;
    
    private JedisPoolConfig pipelinePoolConfig = new JedisPoolConfig();
    {
        pipelinePoolConfig.setMaxTotal(DEFAULT_MAX_POOL_SIZE);
        pipelinePoolConfig.setMaxIdle(DEFAULT_MAX_IDLE);
        pipelinePoolConfig.setMinIdle(DEFAULT_MIN_IDLE);
        pipelinePoolConfig.setMaxWaitMillis(DEFAULT_MAX_WAIT_MILLIS);
    }

    public AtomConfig() {

    }

    public AtomConfig(String host) {
        serverInfo = new ServerInfo();
        serverInfo.setHost(host);
        serverInfo.setPort(DEFAULT_PORT);
    }

    public AtomConfig(String host, int port) {
        serverInfo = new ServerInfo();
        serverInfo.setHost(host);
        serverInfo.setPort(port);
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    public void setServerInfo(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    public String getHost() {
        return serverInfo.getHost();
    }

    public void setHost(String host) {
        serverInfo.setHost(host);
    }

    public int getPort() {
        return serverInfo.getPort();
    }

    public void setPort(int port) {
        serverInfo.setPort(port);
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
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

    public JedisPoolConfig getPipelinePoolConfig() {
        return pipelinePoolConfig;
    }

    public void setPipelinePoolConfig(JedisPoolConfig pipelinePoolConfig) {
        this.pipelinePoolConfig = pipelinePoolConfig;
    }

    @Override
    public String toString() {
        return "AtomConfig [serverInfo=" + serverInfo + ", database=" + database + ", password=" + password
               + ", socketTimeout=" + socketTimeout + ", timeout=" + timeout + ", threadPoolSize=" + threadPoolSize
               + ", clientName=" + clientName + ", pipelinePoolConfig=" + pipelinePoolConfig + "]";
    }

}
