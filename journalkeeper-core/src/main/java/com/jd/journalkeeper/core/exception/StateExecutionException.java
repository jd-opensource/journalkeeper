package com.jd.journalkeeper.core.exception;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class StateExecutionException extends RuntimeException {
    public StateExecutionException(String msg) {
        super(msg);
    }
    public StateExecutionException(Throwable throwable){ super(throwable);}

}
