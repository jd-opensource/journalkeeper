package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesResponse extends BaseResponse {
    private final boolean success;
    private AsyncAppendEntriesResponse(Throwable exception, boolean success, long journalIndex, int term, int entryCount) {
        super(exception);
        this.success = success;
        this.journalIndex = journalIndex;
        this.term = term;
        this.entryCount = entryCount;
    }

    public AsyncAppendEntriesResponse(boolean success, long journalIndex, int term, int entryCount) {
        this(null, success, journalIndex, term, entryCount);    }

    public AsyncAppendEntriesResponse(Throwable exception) {
        this(exception, false, -1L, -1, -1);
    }

    private final long journalIndex;

    private final int term;

    private final int entryCount;

    public long getJournalIndex() {
        return journalIndex;
    }

    public int getTerm() {
        return term;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public boolean isSuccess() {
        return success;
    }
}
