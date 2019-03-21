package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.core.api.StorageEntry;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal<E>  implements Flushable {


    public long minIndex() {
        return 0;
    }

    public long maxIndex() {
        return 0;
    }

    public long flushedIndex() {
        return 0;
    }


    public CompletableFuture<Void> truncate(long givenMax) {
        return null;
    }

    public CompletableFuture<Long> shrink(long givenMin) {
        return null;
    }

    public long append(StorageEntry<E>... entry) {
        return 0L;
    }

    public E read(long index) {
        return null;
    }

    public E [] read(long index, int length) {
        return null;
    }

    public StorageEntry<E> [] readRaw(long index, int length) {
        return null;
    }
    public int getTerm(long index) {
        return 0;
    }

    public void compareOrAppend(StorageEntry<E>[] entries, long index) {

    }

    public void recover() {

    }

    @Override
    public void flush() throws IOException {

    }
}
