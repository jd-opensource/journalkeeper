package com.jd.journalkeeper.core.api;

/**
 * @author liyue25
 * Date: 2019-03-19
 */
public class StorageEntry<E> {
    private final E entry;
    private final int term;
    public StorageEntry(E entry, int term){
        this.entry = entry;
        this.term = term;
    }

    public E getEntry() {
        return entry;
    }

    public int getTerm() {
        return term;
    }
}
