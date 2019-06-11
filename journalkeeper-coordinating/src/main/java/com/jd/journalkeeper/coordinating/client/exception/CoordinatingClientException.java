package com.jd.journalkeeper.coordinating.client.exception;

import com.jd.journalkeeper.coordinating.exception.CoordinatingException;

/**
 * CoordinatingClientException
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingClientException extends CoordinatingException {

    public CoordinatingClientException() {
    }

    public CoordinatingClientException(String message) {
        super(message);
    }

    public CoordinatingClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public CoordinatingClientException(Throwable cause) {
        super(cause);
    }

    public CoordinatingClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}