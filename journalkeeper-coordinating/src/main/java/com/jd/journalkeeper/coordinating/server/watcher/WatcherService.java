package com.jd.journalkeeper.coordinating.server.watcher;

import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.service.Service;

/**
 * WatcherService
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/6
 */
public class WatcherService extends Service {

    private EventBus<TransportEvent> serverEventBus;
    private WatcherManager watcherManager;
    private WatcherPublisher watcherPublisher;
    private WatcherHandler watcherHandler;

    public WatcherService(CoordinatingConfig config, EventBus<TransportEvent> serverEventBus) {
        this.serverEventBus = serverEventBus;
        this.watcherManager = new WatcherManager(serverEventBus);
        this.watcherPublisher = new WatcherPublisher(config.getWatcher(), watcherManager);
        this.watcherHandler = new WatcherHandler(config, watcherManager, watcherPublisher);
    }

    public WatcherManager getWatcherManager() {
        return watcherManager;
    }

    public WatcherHandler getWatcherHandler() {
        return watcherHandler;
    }

    public WatcherPublisher watcherPublisher() {
        return watcherPublisher;
    }
}