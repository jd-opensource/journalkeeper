package com.jd.journalkeeper.coordinating.server.watcher;

import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.coordinating.server.domain.CoordinatingValue;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;

/**
 * WatcherHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/6
 */
public class WatcherHandler {

    private CoordinatingConfig config;
    private WatcherManager watcherManager;
    private WatcherPublisher watcherPublisher;

    public WatcherHandler(CoordinatingConfig config, WatcherManager watcherManager, WatcherPublisher watcherPublisher) {
        this.config = config;
        this.watcherManager = watcherManager;
        this.watcherPublisher = watcherPublisher;
    }

    public boolean addWatcher(byte[] key, Transport transport) {
        return watcherManager.addWatcher(key, transport);
    }

    public boolean removeWatcher(byte[] key, Transport transport) {
        return watcherManager.removeWatcher(key, transport);
    }

    public boolean existWatcher(byte[] key, Transport transport) {
        return watcherManager.existWatcher(key, transport);
    }

    public boolean notifyKeyChanged(byte[] key, CoordinatingValue value) {
        return watcherPublisher.notifyKeyChanged(key, value);
    }

    public boolean notifyKeyRemoved(byte[] key) {
        return watcherPublisher.notifyKeyRemoved(key);
    }
}