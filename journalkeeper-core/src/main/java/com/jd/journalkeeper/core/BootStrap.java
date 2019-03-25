package com.jd.journalkeeper.core;

import com.jd.journalkeeper.core.api.ClusterAccessPoint;
import com.jd.journalkeeper.core.api.JournalKeeperClient;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class BootStrap<E, Q, R> implements ClusterAccessPoint {

    @Override
    public CompletableFuture<JournalKeeperClient> connect(Set servers, Properties properties) {
        return null;
    }
}
