package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.rpc.BaseResponse;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class LastAppliedResponse  extends LeaderResponse {
    private final long lastApplied;
    public LastAppliedResponse(Throwable throwable){
        this(throwable, -1L);
    }

    public LastAppliedResponse(long lastApplied) {
        this(null, lastApplied);
    }

    public LastAppliedResponse(URI leader) {
        super(leader);
        this.lastApplied = -1L;
    }
    private LastAppliedResponse(Throwable exception, long lastApplied) {
        super(exception);
        this.lastApplied = lastApplied;
    }

}
