package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.LeaderResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class UpdateClusterStateResponse extends LeaderResponse {
    public UpdateClusterStateResponse() {
        super();
    }
    public UpdateClusterStateResponse(Throwable exception) {
        super(exception);
    }

}
