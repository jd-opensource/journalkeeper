package com.jd.journalkeeper.coordinating.server.watcher;

import com.jd.journalkeeper.coordinating.server.util.ConcurrentHashSet;
import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportAttribute;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * WatcherManager
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
// TODO 清理空key
public class WatcherManager {

    private static final String WATCHER_KEYS_ATTR = "_WATCHER_KEYS_";

    private EventBus<TransportEvent> serverEventBus;

    private ConcurrentMap<WatcherKey, Set<Transport>> keyMapper = new ConcurrentHashMap<>();

    public WatcherManager(EventBus<TransportEvent> serverEventBus) {
        this.serverEventBus = serverEventBus;
        this.serverEventBus.addListener(new ClearWatcherListener(this));
    }

    public boolean addWatcher(byte[] key, Transport transport) {
        WatcherKey watcherKey = new WatcherKey(key);
        Set<Transport> transports = keyMapper.get(watcherKey);
        if (transports == null) {
            transports = new ConcurrentHashSet<>();
            Set<Transport> oldTransports = keyMapper.putIfAbsent(watcherKey, transports);
            if (oldTransports != null) {
                transports = oldTransports;
            }
        }

        if (!transports.add(transport)) {
            return false;
        }

        addTransportWatcherKey(watcherKey, transport);
        return true;
    }

    public boolean removeWatcher(byte[] key, Transport transport) {
        WatcherKey watcherKey = new WatcherKey(key);
        Set<Transport> transports = keyMapper.get(watcherKey);
        if (transports == null) {
            return false;
        }

        if (!transports.remove(transport)) {
            return false;
        }

        removeTransportWatcherKey(watcherKey, transport);
        return true;
    }

    public boolean existWatcher(byte[] key, Transport transport) {
        Set<Transport> transports = keyMapper.get(new WatcherKey(key));
        if (transport == null) {
            return false;
        }
        return transports.contains(transport);
    }

    public Set<Transport> getWatchers(byte[] key) {
        return keyMapper.get(new WatcherKey(key));
    }

    public Set<WatcherKey> getWatcherKeys(Transport transport) {
        return transport.attr().get(WATCHER_KEYS_ATTR);
    }

    protected void addTransportWatcherKey(WatcherKey watcherKey, Transport transport) {
        getOrCreateTransportWatcherKeys(transport).add(watcherKey);
    }

    protected void removeTransportWatcherKey(WatcherKey watcherKey, Transport transport) {
        getOrCreateTransportWatcherKeys(transport).remove(watcherKey);
    }

    protected Set<WatcherKey> getOrCreateTransportWatcherKeys(Transport transport) {
        TransportAttribute attributes = transport.attr();
        Set<WatcherKey> watcherKeys = attributes.get(WATCHER_KEYS_ATTR);

        if (watcherKeys == null) {
            watcherKeys = new ConcurrentHashSet<>();
            Set<WatcherKey> oldWatcherKeys = attributes.putIfAbsent(WATCHER_KEYS_ATTR, watcherKeys);
            if (oldWatcherKeys != null) {
                watcherKeys = oldWatcherKeys;
            }
        }

        return watcherKeys;
    }
}