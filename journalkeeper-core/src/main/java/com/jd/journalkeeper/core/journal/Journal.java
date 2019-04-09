package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.core.exception.JournalException;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.persistence.TooManyBytesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal  implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);
    private final static int INDEX_STORAGE_SIZE = Long.BYTES;
    private final JournalPersistence indexPersistence;
    private final JournalPersistence journalPersistence;
    private final BufferPool bufferPool;

    public Journal(JournalPersistence indexPersistence, JournalPersistence journalPersistence, BufferPool bufferPool) {
        this.indexPersistence = indexPersistence;
        this.journalPersistence = journalPersistence;
        this.bufferPool = bufferPool;
    }

    /**
     * 最小索引位置
     */
    public long minIndex() {
        return indexPersistence.min() / INDEX_STORAGE_SIZE;
    }

    /**
     * 最大索引位置
     */
    public long maxIndex() {
        return indexPersistence.max() / INDEX_STORAGE_SIZE;
    }

    /**
     * 删除给定索引位置之前的数据。
     * 不保证给定位置之前的数据全都被删除。
     * 保证给定位置（含）之后的数据不会被删除。
     */
    public CompletableFuture<Long> shrink(long givenMin) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                return indexPersistence.shrink(givenMin * INDEX_STORAGE_SIZE);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        })
                .thenApply(min -> {
                    try {
                        return indexPersistence.read(min, INDEX_STORAGE_SIZE);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                })
                .thenApply(ByteBuffer::wrap)
                .thenApply(ByteBuffer::getLong)
                .thenApply(offset -> {
                    try {
                        return journalPersistence.shrink(offset);
                    } catch (IOException e) {
                        throw  new CompletionException(e);
                    }
                })
                .thenApply(offset -> minIndex());
    }

    /**
     * 追加写入StorageEntry
     */
    public long append(StorageEntry storageEntry) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(storageEntry.getLength());
        StorageEntryParser.serialize(byteBuffer, storageEntry);
        return appendRaw(Collections.singletonList(byteBuffer.array()));
    }
    /**
     * 批量追加写入序列化之后的StorageEntry
     */
    public long appendRaw(List<byte []> storageEntries) {
        // 计算索引
        long [] indices = new long[storageEntries.size()];
        long offset = journalPersistence.max();
        for (int i = 0; i < indices.length; i++) {
            indices[i] = offset;
            offset += storageEntries.get(i).length;
        }


        try {
            for (byte [] storageEntry : storageEntries) {
                journalPersistence.append(storageEntry);
            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
        try {
            int indexBufferLength = INDEX_STORAGE_SIZE * storageEntries.size();
            ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferLength);

            for (long index: indices) {
                indexBuffer.putLong(index);
            }
            indexBuffer.flip();
            try {
                indexPersistence.append(indexBuffer.array());
            } catch (TooManyBytesException e) {
                // 如果批量写入超长，改为单条写入
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                for (long index: indices) {
                    buffer.putLong(0, index);
                    indexPersistence.append(buffer.array());
                }
            }
        }  catch (IOException e) {
            throw new JournalException(e);
        }

        return maxIndex();
    }

    /**
     * 给定索引位置读取Entry
     * @return Entry
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public byte [] read(long index){
        checkIndex(index);
        StorageEntry storageEntry = readStorageEntry(index);

        return storageEntry.getEntry();
    }

    public StorageEntry readStorageEntry(long index){
        checkIndex(index);
        try {
            long offset = readOffset(index);

            StorageEntry storageEntry = readHeader(offset);

            byte [] entryBytes = journalPersistence.read(offset + StorageEntryParser.getHeaderLength(), storageEntry.getLength() - StorageEntryParser.getHeaderLength());
            storageEntry.setEntry(entryBytes);

            return storageEntry;
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private byte [] readRawStorageEntry(long index){
        checkIndex(index);
        try {
            long offset = readOffset(index);

            StorageEntry storageEntry = readHeader(offset);

            return journalPersistence.read(offset , storageEntry.getLength());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private StorageEntry readHeader(long offset) {
        try {
            byte [] headerBytes = journalPersistence.read(offset, StorageEntryParser.getHeaderLength());

            return StorageEntryParser.parseHeader(ByteBuffer.wrap(headerBytes));
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private long readOffset(long index) {
        try {
            byte [] indexBytes = indexPersistence.read(index * INDEX_STORAGE_SIZE, INDEX_STORAGE_SIZE);
            return ByteBuffer.wrap(indexBytes).getLong();
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 批量读取Entry
     * @param index 起始索引位置
     * @param size 期望读取的条数
     * @return Entry列表。
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public List<byte []> read(long index, int size) {
        checkIndex(index);
        List<byte []> list = new ArrayList<>(size);
        long i = index;
        while (list.size() < size && i < maxIndex()) {
            list.add(read(i++));
        }
        return list;
    }

    /**
     * 批量读取StorageEntry
     * @param index 起始索引位置
     * @param size 期望读取的条数
     * @return 未反序列化的StorageEntry列表。
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public List<byte []> readRaw(long index, int size) {
        checkIndex(index);
        List<byte []> list = new ArrayList<>(size);
        long i = index;
        while (list.size() < size && i < maxIndex()) {
            list.add(readRawStorageEntry(i++));
        }
        return list;
    }
    /**
     * 读取指定索引位置上Entry的Term。
     * @param index 索引位置。
     * @return Term。
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public int getTerm(long index) {
        if(index == -1) return -1;
        checkIndex(index);
        long offset = readOffset(index);

        StorageEntry storageEntry = readHeader(offset);

        return storageEntry.getTerm();
    }

    /**
     * 从index位置开始：
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目。
     * @param rawEntries 待比较的日志
     * @param startIndex 起始位置
     * @throws IndexUnderflowException 如果 startIndex< minIndex()
     * @throws IndexOverflowException 如果 startIndex >= maxIndex()
     */
    public void compareOrAppendRaw(List<byte []> rawEntries, long startIndex) {

        List<StorageEntry> entries = rawEntries.stream()
                .map(ByteBuffer::wrap)
                .map(StorageEntryParser::parseHeader)
                .collect(Collectors.toList());
        try {
            long index = startIndex;

            for (int i = 0; i < entries.size(); i++, index++) {
                if (index < maxIndex() && getTerm(index) != entries.get(i).getTerm()) {
                    truncate(index);
                }
                if (index == maxIndex()) {
                    appendRaw(rawEntries.subList(i, entries.size()));
                    break;
                }
            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 从索引index位置开始，截掉后面的数据
     * @throws IndexUnderflowException 如果 index < minIndex()
     * @throws IndexOverflowException 如果 index >= maxIndex()
     */
    private void truncate(long index) throws IOException {
        journalPersistence.truncate(readOffset(index));
        indexPersistence.truncate(index * INDEX_STORAGE_SIZE);
    }

    /**
     * 从指定path恢复Journal。
     * 1. 删除journal或者index文件末尾可能存在的不完整的数据。
     * 2. 以Journal为准，修复Index：删除多余的索引，并创建缺失的索引。
     */
    public void recover(Path path, Properties properties) throws IOException {
        Path journalPath = path.resolve("journal");
        Path indexPath = path.resolve("index");
        Files.createDirectories(journalPath);
        Files.createDirectories(indexPath);
        journalPersistence.recover(journalPath, replacePropertiesNames(properties, "^persistence\\.journal\\.(.*)$", "$1"));
        // 截掉末尾半条数据
        truncateJournalTailPartialEntry();

        indexPersistence.recover(indexPath, replacePropertiesNames(properties, "^persistence\\.index\\.(.*)$", "$1"));
        // 截掉末尾半条数据
        indexPersistence.truncate(indexPersistence.max() - indexPersistence.max() % INDEX_STORAGE_SIZE);

        // 删除多余的索引
        truncateExtraIndices();

        // 创建缺失的索引
        buildMissingIndices();

        flush();
        logger.info("Journal recovered, minIndex: {}, maxIndex: {}.", minIndex(), maxIndex());
    }

    private Properties replacePropertiesNames(Properties properties, String fromNameRegex, String toNameRegex ){
        Properties jp = new Properties();
        properties.stringPropertyNames().forEach(k -> {
            String name = k.replaceAll(fromNameRegex, toNameRegex);
            jp.setProperty(name, properties.getProperty(k));
        });
        return jp;
    }

    private void buildMissingIndices() throws IOException {
        // 找到最新一条没有索引位置
        long indexOffset;
        if (indexPersistence.max() - INDEX_STORAGE_SIZE >= indexPersistence.min()) {
            long offset = readOffset(indexPersistence.max() / INDEX_STORAGE_SIZE - 1);
            StorageEntry storageEntry = readHeader(offset);
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
            indexPersistence.append(buffer.array());
            bufferPool.release(buffer);
        }
    }

    private void truncateExtraIndices() throws IOException {

        long position = indexPersistence.max();
        //noinspection StatementWithEmptyBody
        while ((position -= INDEX_STORAGE_SIZE) >= indexPersistence.min() && readOffset(position / INDEX_STORAGE_SIZE) >= journalPersistence.max()) {}
        indexPersistence.truncate(position + INDEX_STORAGE_SIZE);
    }

    private void truncateJournalTailPartialEntry() throws IOException {

        // 找最后的连续2条记录

        long position = journalPersistence.max() - StorageEntryParser.getHeaderLength();
        long lastEntryPosition = -1; // 最后连续2条记录中后面那条的位置
        StorageEntry lastEntry = null;
        while (position >= journalPersistence.min()) {
            try {
                StorageEntry storageEntry = readHeader(position);
                // 找到一条记录的开头位置
                if(lastEntryPosition < 0) { // 之前是否已经找到一条？
                    // 这是倒数第一条，记录之
                    lastEntryPosition = position;
                    lastEntry = storageEntry;
                    if(lastEntryPosition == journalPersistence.min()) {
                        // 只有一条完整的记录也认为OK，保留之。
                        truncatePartialEntry(lastEntryPosition, lastEntry);
                        return;
                    }
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

    private void truncatePartialEntry(long lastEntryPosition, StorageEntry lastEntry) throws IOException {
        // 判断最后一条是否完整
        if(lastEntryPosition + lastEntry.getLength() <= journalPersistence.max()) {
            // 完整，截掉后面的部分
            journalPersistence.truncate(lastEntryPosition + lastEntry.getLength());
        } else {
            // 不完整，直接截掉这条数据
            journalPersistence.truncate(lastEntryPosition);
        }
    }

    /**
     * 将所有内存中的Journal和Index写入磁盘。
     */
    @Override
    public void flush() {
        while (journalPersistence.flushed() < journalPersistence.max() ||
            indexPersistence.flushed() < indexPersistence.max())
        try {
            journalPersistence.flush();
            indexPersistence.flush();
        } catch (IOException e) {
            throw new JournalException(e);
        }
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
