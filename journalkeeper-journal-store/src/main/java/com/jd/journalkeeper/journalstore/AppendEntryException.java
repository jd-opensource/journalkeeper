package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class AppendEntryException extends RuntimeException {
    public AppendEntryException(String message) {
        super(message);
    }
}
