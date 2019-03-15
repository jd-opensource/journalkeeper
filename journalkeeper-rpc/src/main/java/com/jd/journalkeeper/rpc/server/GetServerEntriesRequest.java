package com.jd.journalkeeper.rpc.server;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServerEntriesRequest {
    private final long index;
    private final int maxSize;

    public GetServerEntriesRequest(long index, int maxSize) {
        this.index = index;
        this.maxSize = maxSize;
    }

    public long getIndex() {
        return index;
    }

    public int getMaxSize() {
        return maxSize;
    }
}
