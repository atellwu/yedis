package com.yeahmobi.yedis.group;

import java.util.List;

import com.yeahmobi.yedis.common.ServerInfo;

public interface MasterSlaveConfigManager {

    ServerInfo getMasterServerInfo();

    List<ServerInfo> getSlaveServerInfos();

    void addListener(ConfigChangeListener listener);

    void close();
}
