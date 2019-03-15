package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.client.GetServersResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServerEntriesResponse<E> extends BaseResponse {
    private final E [] entries;
    private final long minIndex;
    private final long commitIndex;

    public GetServerEntriesResponse(Throwable exception) {
        this(exception, null, -1L, -1L);
    }

    public GetServerEntriesResponse(E[] entries, long minIndex, long commitIndex) {
        this(null, entries, minIndex, commitIndex);
    }

    private GetServerEntriesResponse(Throwable exception, E[] entries, long minIndex, long commitIndex) {
        super(exception);
        this.entries = entries;
        this.minIndex = minIndex;
        this.commitIndex = commitIndex;
    }

    public E[] getEntries() {
        return entries;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public long getCommitIndex() {
        return commitIndex;
    }
}
