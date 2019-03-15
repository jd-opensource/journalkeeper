package com.jd.journalkeeper.rpc;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public abstract class BaseResponse {
    private final Throwable exception;

    protected BaseResponse(Throwable exception) {
        this.exception = exception;
    }


    public Throwable getException() {
        return exception;
    }

}
