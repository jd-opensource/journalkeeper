package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalStoreQuery {
    private final long index;
    private final int size;

    public JournalStoreQuery(long index, int size) {
        this.index = index;
        this.size = size;
    }

    public long getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }
}
