package com.jd.journalkeeper.utils.threads;

/**
 * @author liyue25
 * Date: 2019-06-21
 */
public interface ExceptionHandler {
    boolean handleException(Throwable t);
}
