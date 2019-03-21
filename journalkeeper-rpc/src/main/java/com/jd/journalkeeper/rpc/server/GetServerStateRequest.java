package com.jd.journalkeeper.rpc.server;

/**
 * @author liyue25
 * Date: 2019-03-21
 */
public class GetServerStateRequest {
    private final long lastIncludedIndex;
    private final long offset;

    public GetServerStateRequest(long lastIncludedIndex, long offset) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.offset = offset;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public long getOffset() {
        return offset;
    }
}
