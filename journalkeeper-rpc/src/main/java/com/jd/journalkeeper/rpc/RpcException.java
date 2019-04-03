package com.jd.journalkeeper.rpc;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class RpcException extends RuntimeException {
    public RpcException(BaseResponse response) {
        super(response.getError());
    }
}
