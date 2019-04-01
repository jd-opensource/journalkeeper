package com.jd.journalkeeper.rpc;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class RpcException extends RuntimeException {
    public RpcException(Throwable t) {
        super(t);
    }
}
