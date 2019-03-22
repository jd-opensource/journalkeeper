package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.StorageEntry;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.JournalPersistence;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal<E>  implements Flushable, Closeable {

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

    public CompletableFuture<Long> shrink(long givenMin) {

        return indexPersistence.shrink(givenMin * INDEX_STORAGE_SIZE)
                .thenApply(min -> indexPersistence.read(min, INDEX_STORAGE_SIZE))
                .thenApply(ByteBuffer::getLong)
                .thenCompose(journalPersistence::shrink)
                .thenApply(offset -> minIndex());
    }

    public long append(StorageEntry<E> storageEntry) {
        return append(Collections.singletonList(storageEntry));
    }

    public long append(List<StorageEntry<E>> storageEntries) {
        // 计算长度
        int journalBufferLength = storageEntries.stream()
                .peek(storageEntry -> storageEntry.setLength(serializer.sizeOf(storageEntry.getEntry())))
                .mapToInt(StorageEntry::getLength).sum();
        // 计算索引
        long [] indices = new long[storageEntries.size()];
        long offset = journalPersistence.max();
        for (int i = 0; i < indices.length; i++) {
            indices[i] = offset;
            offset += storageEntries.get(i).getLength();
        }

        ByteBuffer journalBuffer = bufferPool.allocate(journalBufferLength);
        try {
            for (StorageEntry<E> storageEntry : storageEntries) {
                StorageEntryParser.serialze(journalBuffer, storageEntry, serializer);
            }
            journalBuffer.flip();
            journalPersistence.append(journalBuffer);
        } finally {
            bufferPool.release(journalBuffer);
        }
        int indexBufferLength = INDEX_STORAGE_SIZE * storageEntries.size();
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
        checkIndex(index);
        StorageEntry<E> storageEntry = readStorageEntry(index);

        return storageEntry.getEntry();
    }

    private StorageEntry<E> readStorageEntry(long index) {
        checkIndex(index);
        long offset = readOffset(index);

        StorageEntry<E> storageEntry = readHeader(offset);

        ByteBuffer entryBuffer = journalPersistence.read(offset + StorageEntryParser.getHeaderLength(), storageEntry.getLength());
        storageEntry.setEntry(serializer.parse(entryBuffer));

        return storageEntry;
    }

    private StorageEntry<E> readHeader(long offset) {
        ByteBuffer headerBuffer = journalPersistence.read(offset, StorageEntryParser.getHeaderLength());

        return StorageEntryParser.parse(headerBuffer);
    }

    private long readOffset(long index) {
        ByteBuffer indexBuffer = indexPersistence.read(index * INDEX_STORAGE_SIZE, INDEX_STORAGE_SIZE);
        return indexBuffer.getLong();
    }

    public List<E> read(long index, int length) {
        checkIndex(index);
        List<E> list = new ArrayList<>(length);
        long i = index;
        while (list.size() < length && i < maxIndex()) {
            list.add(read(i++));
        }
        return list;
    }

    public List<StorageEntry<E>> readRaw(long index, int length) {
        checkIndex(index);
        List<StorageEntry<E>> list = new ArrayList<>(length);
        long i = index;
        while (list.size() < length && i < maxIndex()) {
            list.add(readStorageEntry(i++));
        }
        return list;
    }
    public int getTerm(long index) {
        checkIndex(index);
        long offset = readOffset(index);

        StorageEntry<E> storageEntry = readHeader(offset);

        return storageEntry.getTerm();
    }

    /**
     * 从index位置开始：
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目
     * @param entries 待比较的日志
     * @param startIndex 起始位置
     */
    public void compareOrAppend(List<StorageEntry<E>> entries, long startIndex) {

        long index = startIndex;

        for (int i = 0; i < entries.size(); i++, index++) {
            if (index < maxIndex() && getTerm(index) != entries.get(i).getTerm()) {
                truncate(index);
            }
            if(index == maxIndex()) {
                append(entries.subList(i, entries.size()));
                break;
            }
        }
    }

    /**
     * 从索引index位置开始，截掉后面的数据
     */
    private void truncate(long index) {
        journalPersistence.truncate(readOffset(index));
        indexPersistence.truncate(index * INDEX_STORAGE_SIZE);
    }

    public void recover(Path path, Properties properties) throws IOException {
        journalPersistence.recover(path.resolve("journal"), properties);
        // 截掉末尾半条数据
        truncateJournalTailPartialEntry();

        indexPersistence.recover(path.resolve("index"), properties);
        // 截掉末尾半条数据
        indexPersistence.truncate(indexPersistence.max() - indexPersistence.min() % INDEX_STORAGE_SIZE);

        // 删除多余的索引
        truncateExtraIndices();

        // 创建缺失的索引
        buildMissingIndices();

        journalPersistence.flush();
        indexPersistence.flush();
    }

    private void buildMissingIndices() {
        // 找到最新一条没有索引位置
        long indexOffset;
        if (indexPersistence.max() - INDEX_STORAGE_SIZE >= indexPersistence.min()) {
            long offset = readOffset(indexPersistence.max() / INDEX_STORAGE_SIZE - 1);
            StorageEntry<E> storageEntry = readHeader(offset);
            indexOffset = offset + storageEntry.getLength();
        } else {
            indexOffset = journalPersistence.min();
        }

        // 创建索引
        List<Long> indices = new LinkedList<>();
        while (indexOffset < journalPersistence.max()) {
            indices.add(indexOffset);
            indexOffset += readHeader(indexOffset).getLength();
        }

        // 写入索引
        if(!indices.isEmpty()) {
            ByteBuffer buffer = bufferPool.allocate(indices.size() * INDEX_STORAGE_SIZE);
            indices.forEach(buffer::putLong);
            buffer.flip();
            indexPersistence.append(buffer);
            bufferPool.release(buffer);
        }
    }

    private void truncateExtraIndices() {

        long position = indexPersistence.max();
        //noinspection StatementWithEmptyBody
        while ((position -= INDEX_STORAGE_SIZE) >= indexPersistence.min() && readOffset(position) < journalPersistence.max()) {}
        indexPersistence.truncate(position + INDEX_STORAGE_SIZE);
    }

    private void truncateJournalTailPartialEntry() {

        // 找最后的连续2条记录

        long position = journalPersistence.max() - StorageEntryParser.getHeaderLength();
        long lastEntryPosition = -1; // 最后连续2条记录中后面那条的位置
        StorageEntry<E> lastEntry = null;
        while (position >= journalPersistence.min()) {
            try {
                StorageEntry<E> storageEntry = readHeader(position);
                // 找到一条记录的开头位置
                if(lastEntryPosition < 0) { // 之前是否已经找到一条？
                    // 这是倒数第一条，记录之
                    lastEntryPosition = position;
                    lastEntry = storageEntry;
                } else {
                    // 这是倒数第二条
                    if(position + storageEntry.getLength() == lastEntryPosition) {
                        // 找到最后2条中位置较小的那条，并且较小那条的位置+长度==较大那条的位置
                        truncatePartialEntry(lastEntryPosition, lastEntry);
                        return;
                    } else { // 之前找到的那个第一条是假的（小概率会出现Entry中恰好也有连续2个字节等于MAGIC）
                        lastEntryPosition = position;
                        lastEntry = storageEntry;
                    }
                }
            } catch (Exception ignored) {}
            position --;
        }

        // 找到最小位置了，啥也没找到，直接清空所有数据。
        journalPersistence.truncate(journalPersistence.min());
    }

    private void truncatePartialEntry(long lastEntryPosition, StorageEntry<E> lastEntry) {
        // 判断最后一条是否完整
        if(lastEntryPosition + lastEntry.getLength() <= journalPersistence.max()) {
            // 完整，截掉后面的部分
            journalPersistence.truncate(lastEntryPosition + lastEntry.getLength());
        } else {
            // 不完整，直接截掉这条数据
            journalPersistence.truncate(lastEntryPosition);
        }
    }

    @Override
    public void flush() throws IOException {
        journalPersistence.flush();
        indexPersistence.flush();
    }

    private void checkIndex(long index) {
        long position = index * INDEX_STORAGE_SIZE;
        if(position < indexPersistence.min()) {
            throw new IndexUnderflowException();
        }
        if(position >= indexPersistence.max()) {
            throw new IndexOverflowException();
        }
    }

    @Override
    public void close() throws IOException {
        indexPersistence.close();
        journalPersistence.close();
    }


}
