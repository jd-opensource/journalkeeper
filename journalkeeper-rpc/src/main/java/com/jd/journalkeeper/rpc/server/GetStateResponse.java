package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetStateResponse extends BaseResponse {
//    private final S state;
    private final long lastApplied;

    public GetStateResponse(Throwable t) {
        super(t);
//        state = null;
        lastApplied = -1L;
    }

//    public GetStateResponse(S state){
//        this(state, -1L);
//    }

//    public GetStateResponse(S state, long lastApplied) {
//        super(null);
//        this.state = state;
//        this.lastApplied = lastApplied;
//    }
//
//    public S getState() {
//        return state;
//    }

    public long getLastApplied() {
        return lastApplied;
    }
}
