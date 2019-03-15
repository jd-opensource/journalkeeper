package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class RequestVoteResponse  extends BaseResponse {
    public RequestVoteResponse(Throwable exception) {
        super(exception);
    }
}
