package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.event.EventWatcher;

import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClusterAccessPoint<Q,R,E> {
    CompletableFuture<JournalKeeperClient<Q, R, E>> connect(Set<URI> servers, Properties properties);
    void watch(EventWatcher eventWatcher);
    void unwatch(EventWatcher eventWatcher);
}
