package com.jd.journalkeeper.core.exception;

/**
 *
 * @author liyue25
 * Date: 2019-03-25
 */
public class ServerBusyException extends RuntimeException {
    // TODO: LEADER队列满时抛出此异常
    public ServerBusyException(String msg) {
        super(msg);
    }
    public ServerBusyException(Throwable throwable){ super(throwable);}

}
