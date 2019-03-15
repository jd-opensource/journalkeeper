package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesResponse extends BaseResponse {
    public AsyncAppendEntriesResponse(Throwable exception) {
        super(exception);
    }
}
