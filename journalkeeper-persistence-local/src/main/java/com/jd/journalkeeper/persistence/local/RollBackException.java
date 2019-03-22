package com.jd.journalkeeper.persistence.local;

/**
 * @author liyue25
 * Date: 2018/8/27
 */
public class RollBackException extends RuntimeException {
    public RollBackException(String message) {
        super(message);
    }
}
