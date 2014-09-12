package com.yeahmobi.yedis.common;

public class ServerInfo {

    private String host;
    private int    port;

    public ServerInfo() {
    }

    public ServerInfo(String host, int port) {
        super();
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String generateKey() {
        return host + "_" + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ServerInfo other = (ServerInfo) obj;
        if ((this.host == null) ? (other.host != null) : !this.host.equals(other.host)) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + host.hashCode();
        hash = 41 * hash + this.port;
        return hash;
    }

    @Override
    public String toString() {
        return String.format("ServerInfo [host=%s, port=%s]", host, port);
    }

}
