package com.jd.journalkeeper.core.exception;

public class JournalException extends RuntimeException {
    public JournalException(String message) {
        super(message);
    }
    public JournalException(){
        super();
    }
    public JournalException(String message, Throwable t) {
        super(message, t);
    }

    public JournalException(Throwable t) {
        super(t);
    }
}
