package com.jd.journalkeeper.core.api;

import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClusterAccessPoint<E, Q, R> {
    CompletableFuture<JournalKeeperClient<E, Q, R>> connect(Set<URI> servers, Properties properties);

}
