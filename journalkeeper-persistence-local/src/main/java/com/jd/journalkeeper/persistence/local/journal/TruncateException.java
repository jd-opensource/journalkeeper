package com.jd.journalkeeper.persistence.local.journal;

/**
 * @author liyue25
 * Date: 2018/8/27
 */
class TruncateException extends RuntimeException {
    TruncateException(String message) {
        super(message);
    }
}
