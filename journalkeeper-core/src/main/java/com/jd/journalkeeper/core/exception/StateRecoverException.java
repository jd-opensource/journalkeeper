package com.jd.journalkeeper.core.exception;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class StateRecoverException extends RuntimeException {
    public StateRecoverException(String msg) {
        super(msg);
    }
    public StateRecoverException(Throwable throwable){ super(throwable);}

}
