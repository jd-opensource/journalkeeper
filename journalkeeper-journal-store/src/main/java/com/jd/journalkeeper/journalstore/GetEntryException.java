package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class GetEntryException extends RuntimeException {
    public GetEntryException(String message) {
        super(message);
    }

    public GetEntryException(Throwable throwable) {
        super(throwable);
    }
}
