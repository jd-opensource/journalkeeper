package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServerEntriesResponse extends BaseResponse {
    private final List<byte []> entries;
    private final long minIndex;
    private final long lastApplied;

    public GetServerEntriesResponse(Throwable exception) {
        this(exception, null, -1L, -1L);
    }

    public GetServerEntriesResponse(List<byte []> entries, long minIndex, long lastApplied) {
        this(null, entries, minIndex, lastApplied);
    }

    private GetServerEntriesResponse(Throwable exception, List<byte []> entries, long minIndex, long lastApplied) {
        super(exception);
        this.entries = entries;
        this.minIndex = minIndex;
        this.lastApplied = lastApplied;
    }

    public List<byte []> getEntries() {
        return entries;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
