package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateResponse<R>  extends BaseResponse {
    private final R result;
    private final long lastApplied;
    public QueryStateResponse(Throwable t) {
        this(null, -1,  t);
    }

    public QueryStateResponse(R result, long lastApplied){
        this(result, lastApplied, null);
    }
    public QueryStateResponse(R result){
        this(result, -1L, null);
    }

    private QueryStateResponse(R result, long lastApplied, Throwable t) {
        super(t);
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
