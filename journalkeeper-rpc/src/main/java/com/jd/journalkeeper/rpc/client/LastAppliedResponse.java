package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class LastAppliedResponse  extends BaseResponse {
    public LastAppliedResponse(Throwable exception) {
        super(exception);
    }
}
