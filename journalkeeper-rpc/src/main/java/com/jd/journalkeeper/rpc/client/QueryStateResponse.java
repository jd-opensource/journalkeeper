package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateResponse<R>  extends BaseResponse {
    private final R result;
    private final long lastApplied;

    public QueryStateResponse(Throwable t) {
        super(t);
        result = null;
        lastApplied = -1L;
    }

    public QueryStateResponse(R result){
        this(result, -1L);
    }

    public QueryStateResponse(R result, long lastApplied) {
        super(null);
        this.result = result;
        this.lastApplied = lastApplied;
    }

    public R getResult() {
        return result;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
