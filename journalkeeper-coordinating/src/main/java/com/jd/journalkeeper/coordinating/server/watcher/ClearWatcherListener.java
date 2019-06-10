package com.jd.journalkeeper.coordinating.server.watcher;

import com.jd.journalkeeper.rpc.remoting.concurrent.EventListener;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.event.TransportEventType;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;

import java.util.Set;

/**
 * ClearWatcherListener
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/6
 */
public class ClearWatcherListener implements EventListener<TransportEvent> {

    private WatcherManager watcherManager;

    public ClearWatcherListener(WatcherManager watcherManager) {
        this.watcherManager = watcherManager;
    }

    @Override
    public void onEvent(TransportEvent event) {
        if (!event.getType().equals(TransportEventType.CLOSE)) {
            return;
        }

        Transport transport = event.getTransport();
        Set<WatcherKey> watcherKeys = watcherManager.getWatcherKeys(transport);

        if (watcherKeys != null) {
            for (WatcherKey watcherKey : watcherKeys) {
                watcherManager.removeWatcher(watcherKey.getKey(), transport);
            }
        }
    }
}