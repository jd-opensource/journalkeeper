package com.jd.journalkeeper.exceptions;

/**
 *
 * @author liyue25
 * Date: 2019-03-25
 */
public class ServerBusyException extends RuntimeException {
    public ServerBusyException(){
        super();
    }
    public ServerBusyException(String msg) {
        super(msg);
    }
    public ServerBusyException(Throwable throwable){ super(throwable);}

}
