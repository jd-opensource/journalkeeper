package com.jd.journalkeeper.coordinating.server;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;

/**
 * CoordinatingContext
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingContext {

    private CoordinatingConfig config;
    private CoordinatingKeeperServer keeperServer;

    public CoordinatingContext(CoordinatingConfig config, CoordinatingKeeperServer keeperServer) {
        this.config = config;
        this.keeperServer = keeperServer;
    }

    public CoordinatingConfig getConfig() {
        return config;
    }

    public CoordinatingKeeperServer getKeeperServer() {
        return keeperServer;
    }
}