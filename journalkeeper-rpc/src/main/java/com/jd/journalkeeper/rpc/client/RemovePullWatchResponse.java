package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;

/**
 * @author liyue25
 * Date: 2019-04-22
 */
public class RemovePullWatchResponse extends BaseResponse {
    public RemovePullWatchResponse() {
        super(StatusCode.SUCCESS);
    }
    public RemovePullWatchResponse(Throwable throwable) {super(throwable);}
}
