package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class QueryJournalStoreException extends RuntimeException {
    public QueryJournalStoreException(String message) {
        super(message);
    }

    public QueryJournalStoreException(Throwable throwable) {
        super(throwable);
    }
}
