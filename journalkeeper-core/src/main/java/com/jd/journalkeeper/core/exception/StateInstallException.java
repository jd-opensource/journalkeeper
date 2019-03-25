package com.jd.journalkeeper.core.exception;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class StateInstallException extends RuntimeException {
    public StateInstallException(String msg) {
        super(msg);
    }
    public StateInstallException(){}
    public StateInstallException(Throwable throwable){super(throwable);}

}
