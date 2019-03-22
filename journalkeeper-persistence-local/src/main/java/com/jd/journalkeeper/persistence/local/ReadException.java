package com.jd.journalkeeper.persistence.local;

/**
 * @author liyue25
 * Date: 2019-03-22
 */
public class ReadException extends RuntimeException {
    public ReadException(String message) {
        super(message);
    }
    public ReadException(){
        super();
    }
    public ReadException(String message, Throwable t) {
        super(message, t);
    }

    public ReadException(Throwable t) {
        super(t);
    }
}
