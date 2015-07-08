package com.yeahmobi.yedis.group;

import java.util.List;

import com.yeahmobi.yedis.common.ServerInfo;

/**
 * @author atell
 */
public class DefaultConfigManager implements MasterSlaveConfigManager {

    private ServerInfo       writeSeverInfo;

    private List<ServerInfo> readSeverInfoList;

    public DefaultConfigManager(ServerInfo writeSeverInfo, List<ServerInfo> readSeverInfoList) {
        this.writeSeverInfo = writeSeverInfo;
        this.readSeverInfoList = readSeverInfoList;
    }

    @Override
    public ServerInfo getMasterServerInfo() {
        return this.writeSeverInfo;
    }

    @Override
    public List<ServerInfo> getSlaveServerInfos() {
        return this.readSeverInfoList;
    }

    @Override
    public void addListener(ConfigChangeListener listener) {
    }

    @Override
    public void close() {
    }

}
