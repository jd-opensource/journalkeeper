package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

import java.nio.ByteBuffer;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServerStateResponse extends BaseResponse {
//    private final S state;
    private final long lastIncludedIndex;
    private final int lastIncludedTerm;
    private final long offset;
    private final byte [] data;
    private final boolean done;

    private GetServerStateResponse(Throwable exception, long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done) {
        super(exception);
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }
    public GetServerStateResponse(Throwable exception) {
        this(exception, -1L, -1, -1L, null, false);
    }
    public GetServerStateResponse(long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done) {
        this(null, lastIncludedIndex, lastIncludedTerm, offset, data, done);
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }
}
