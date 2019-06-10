package com.jd.journalkeeper.coordinating.server.config;

import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;

import java.util.Properties;

/**
 * CoordinatingConfig
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingConfig {

    private ServerConfig server;
    private CoordinatingKeeperConfig state;
    private CoordinatingWatcherConfig watcher;
    private Properties properties;

    public void setServer(ServerConfig server) {
        this.server = server;
    }

    public ServerConfig getServer() {
        return server;
    }

    public void setKeeper(CoordinatingKeeperConfig state) {
        this.state = state;
    }

    public CoordinatingKeeperConfig getKeeper() {
        return state;
    }

    public void setWatcher(CoordinatingWatcherConfig watcher) {
        this.watcher = watcher;
    }

    public CoordinatingWatcherConfig getWatcher() {
        return watcher;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "CoordinatingConfig{" +
                "server=" + server +
                ", state=" + state +
                ", properties=" + properties +
                '}';
    }
}