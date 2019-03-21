package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.StorageEntry;
import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.JournalPersistence;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal<E>  implements Flushable {

    public final static int INDEX_STORAGE_SIZE = Long.BYTES;
    private final JournalPersistence indexPersistence;
    private final JournalPersistence journalPersistence;
    private final Serializer<E> serializer;
    private final BufferPool bufferPool;

    public Journal(JournalPersistence indexPersistence, JournalPersistence journalPersistence, Serializer<E> serializer, BufferPool bufferPool) {
        this.indexPersistence = indexPersistence;
        this.journalPersistence = journalPersistence;
        this.serializer = serializer;
        this.bufferPool = bufferPool;
    }


    public long minIndex() {
        return indexPersistence.max() / INDEX_STORAGE_SIZE;
    }

    public long maxIndex() {
        return indexPersistence.min() / INDEX_STORAGE_SIZE;
    }

    public long flushedIndex() {
        return indexPersistence.flushed() / INDEX_STORAGE_SIZE;
    }

    public CompletableFuture<Long> shrink(long givenMin) {

        return indexPersistence.shrink(givenMin * INDEX_STORAGE_SIZE)
                .thenApply(min -> indexPersistence.read(min, INDEX_STORAGE_SIZE))
                .thenApply(ByteBuffer::getLong)
                .thenCompose(journalPersistence::shrink)
                .thenApply(offset -> minIndex());
    }

    @SafeVarargs
    public final long append(StorageEntry<E>... storageEntries) {
        // 计算长度
        int journalBufferLength = Arrays.stream(storageEntries)
                .peek(storageEntry -> storageEntry.setLength(serializer.sizeOf(storageEntry.getEntry())))
                .mapToInt(StorageEntry::getLength).sum();
        // 计算索引
        long [] indices = new long[storageEntries.length];
        long offset = journalPersistence.max();
        for (int i = 0; i < indices.length; i++) {
            indices[i] = offset;
            offset += storageEntries[i].getLength();
        }

        ByteBuffer journalBuffer = bufferPool.allocate(journalBufferLength);
        try {
            for (StorageEntry<E> storageEntry : storageEntries) {
                storageEntry.writeHeader(journalBuffer);
                serializer.serialize(journalBuffer, storageEntry.getEntry());
            }
            journalBuffer.flip();
            journalPersistence.append(journalBuffer);
        } finally {
            bufferPool.release(journalBuffer);
        }
        int indexBufferLength = INDEX_STORAGE_SIZE * storageEntries.length;
        ByteBuffer indexBuffer = bufferPool.allocate(indexBufferLength);
        try {
            for (long index: indices) {
                indexBuffer.putLong(index);
            }
            indexBuffer.flip();
            indexPersistence.append(indexBuffer);
        } finally {
            bufferPool.release(indexBuffer);
        }

        return maxIndex();
    }

    public E read(long index) {
        StorageEntry<E> storageEntry = readStorageEntry(index);

        return storageEntry.getEntry();
    }

    private StorageEntry<E> readStorageEntry(long index) {
        ByteBuffer indexBuffer = indexPersistence.read(index * INDEX_STORAGE_SIZE, INDEX_STORAGE_SIZE);
        long offset = indexBuffer.getLong();

        ByteBuffer lengthBuffer = journalPersistence.read(offset, Integer.BYTES);
        int length = lengthBuffer.getInt();

        ByteBuffer journalBuffer = journalPersistence.read(offset, length);

        return StorageEntry.parse(journalBuffer, serializer);
    }

    public List<E> read(long index, int length) {
        List<E> list = new ArrayList<>(length);
        long i = index;
        while (list.size() < length && i < maxIndex()) {
            list.add(read(i++));
        }
        return list;
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
