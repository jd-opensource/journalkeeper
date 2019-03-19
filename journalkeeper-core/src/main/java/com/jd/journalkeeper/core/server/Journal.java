package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.core.api.StorageEntry;

import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal<E>  {
    public long minIndex() {
        return 0;
    }

    public long maxIndex() {
        return 0;
    }

    public long flushedIndex() {
        return 0;
    }

    public CompletableFuture<Long> flush() {
        return null;
    }

    public CompletableFuture<Void> truncate(long givenMax) {
        return null;
    }

    public CompletableFuture<Long> shrink(long givenMin) {
        return null;
    }

    public CompletableFuture<Long> append(StorageEntry<E>... entries) {
        return null;
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
}
