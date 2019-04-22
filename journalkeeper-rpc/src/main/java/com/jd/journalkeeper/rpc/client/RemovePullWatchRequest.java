package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-04-22
 */
public class RemovePullWatchRequest {
    private final long pullWatchId;

    public RemovePullWatchRequest(long pullWatchId) {
        this.pullWatchId = pullWatchId;
    }

    public long getPullWatchId() {
        return pullWatchId;
    }
}
