package com.jd.journalkeeper.coordinating.exception;

/**
 * CoordinatingException
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingException extends RuntimeException {

    public CoordinatingException() {
    }

    public CoordinatingException(String message) {
        super(message);
    }

    public CoordinatingException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoordinatingException(Throwable cause) {
        super(cause);
    }

    public CoordinatingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}