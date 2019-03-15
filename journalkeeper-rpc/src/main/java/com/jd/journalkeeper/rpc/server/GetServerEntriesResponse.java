package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServerEntriesResponse<E> extends BaseResponse {
    private final E [] entries;
    private final long minIndex;
    private final long lastApplied;

    public GetServerEntriesResponse(Throwable exception) {
        this(exception, null, -1L, -1L);
    }

    public GetServerEntriesResponse(E[] entries, long minIndex, long lastApplied) {
        this(null, entries, minIndex, lastApplied);
    }

    private GetServerEntriesResponse(Throwable exception, E[] entries, long minIndex, long lastApplied) {
        super(exception);
        this.entries = entries;
        this.minIndex = minIndex;
        this.lastApplied = lastApplied;
    }

    public E[] getEntries() {
        return entries;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
