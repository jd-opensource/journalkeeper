package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class QueryStateResponse  extends BaseResponse {
    private final byte [] result;
    private final long lastApplied;
    public QueryStateResponse(Throwable t) {
        this(null, -1,  t);
    }

    public QueryStateResponse(byte [] result, long lastApplied){
        this(result, lastApplied, null);
    }
    public QueryStateResponse(byte [] result){
        this(result, -1L, null);
    }

    private QueryStateResponse(byte [] result, long lastApplied, Throwable t) {
        super(t);
        this.result = result;
        this.lastApplied = lastApplied;
    }
    public byte [] getResult() {
        return result;
    }

    public long getLastApplied() {
        return lastApplied;
    }
}
