package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class LastAppliedResponse  extends BaseResponse {
    private final long lastApplied;
    public LastAppliedResponse(Throwable throwable){
        this(throwable, -1L);
    }

    public LastAppliedResponse(long lastApplied) {
        this(null, lastApplied);
    }

    private LastAppliedResponse(Throwable exception, long lastApplied) {
        super(exception);
        this.lastApplied = lastApplied;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
