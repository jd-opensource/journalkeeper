package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.exceptions.ServerBusyException;
import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.LeaderResponse;
import com.jd.journalkeeper.rpc.StatusCode;

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

    @Override
    public void setException(Throwable throwable) {
        try {
            throw throwable;
        } catch (ServerBusyException e) {
            setStatusCode(StatusCode.SERVER_BUSY);
        } catch (Throwable t) {
            super.setException(throwable);
        }
    }

}
