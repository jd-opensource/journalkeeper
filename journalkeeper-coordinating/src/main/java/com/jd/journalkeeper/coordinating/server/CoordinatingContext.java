package com.jd.journalkeeper.coordinating.server;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.coordinating.server.watcher.WatcherHandler;
import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;

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
    private EventBus<TransportEvent> serverEventBus;
    private WatcherHandler watcherHandler;

    public CoordinatingContext(CoordinatingConfig config, CoordinatingKeeperServer keeperServer, Serializer serializer,
                               EventBus<TransportEvent> serverEventBus, WatcherHandler watcherHandler) {
        this.config = config;
        this.keeperServer = keeperServer;
        this.serializer = serializer;
        this.serverEventBus = serverEventBus;
        this.watcherHandler = watcherHandler;
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

    public EventBus<TransportEvent> getServerEventBus() {
        return serverEventBus;
    }

    public WatcherHandler getWatcherHandler() {
        return watcherHandler;
    }
}