package com.jd.journalkeeper.rpc.client;

/**
 * @author liyue25
 * Date: 2019-04-22
 */
public class PullEventsRequest {
    private final long pullWatchId;
    private final long ackSequence;

    public PullEventsRequest(long pullWatchId, long ackSequence) {
        this.pullWatchId = pullWatchId;
        this.ackSequence = ackSequence;
    }

    public long getPullWatchId() {
        return pullWatchId;
    }

    public long getAckSequence() {
        return ackSequence;
    }
}
