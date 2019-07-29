package com.jd.journalkeeper.coordinating.state.exception;

import com.jd.journalkeeper.coordinating.exception.CoordinatingException;

/**
 * CoordinatingStateException
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingStateException extends CoordinatingException {

    public CoordinatingStateException() {
    }

    public CoordinatingStateException(String message) {
        super(message);
    }

    public CoordinatingStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoordinatingStateException(Throwable cause) {
        super(cause);
    }

    public CoordinatingStateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}