package com.jd.journalkeeper.coordinating.server;

import com.jd.journalkeeper.base.Serializer;
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
    private Serializer serializer;

    public CoordinatingContext(CoordinatingConfig config, CoordinatingKeeperServer keeperServer, Serializer serializer) {
        this.config = config;
        this.keeperServer = keeperServer;
        this.serializer = serializer;
    }

    public CoordinatingConfig getConfig() {
        return config;
    }

    public CoordinatingKeeperServer getKeeperServer() {
        return keeperServer;
    }

    public Serializer getSerializer() {
        return serializer;
    }
}