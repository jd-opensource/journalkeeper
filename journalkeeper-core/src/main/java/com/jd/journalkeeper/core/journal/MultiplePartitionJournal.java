package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.core.exception.JournalException;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.persistence.TooManyBytesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class MultiplePartitionJournal implements Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(MultiplePartitionJournal.class);
    private final static int INDEX_STORAGE_SIZE = Long.BYTES;
    private final JournalPersistence indexPersistence;
    private final JournalPersistence journalPersistence;
    private final Map<Short, JournalPersistence> partitionMap;
    private final PersistenceFactory persistenceFactory;
    private final BufferPool bufferPool;

    public MultiplePartitionJournal(PersistenceFactory persistenceFactory, BufferPool bufferPool) {
        this.indexPersistence = persistenceFactory.createJournalPersistenceInstance();
        this.journalPersistence = persistenceFactory.createJournalPersistenceInstance();
        this.persistenceFactory = persistenceFactory;
        this.partitionMap = new ConcurrentHashMap<>();
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
     * TODO: 支持多个partition
     * 删除给定索引位置之前的数据。
     * 不保证给定位置之前的数据全都被删除。
     * 保证给定位置（含）之后的数据不会被删除。
     */
    public CompletableFuture<Long> compact(long givenMinIndex) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                return indexPersistence.compact(givenMinIndex * INDEX_STORAGE_SIZE);
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
                        return journalPersistence.compact(offset);
                    } catch (IOException e) {
                        throw  new CompletionException(e);
                    }
                }).thenApply(minOffset -> CompletableFuture.allOf(
                        partitionMap.values().stream()
                                .map(pp -> CompletableFuture.runAsync(() -> compactPartition(pp, minOffset)))
                                .toArray(CompletableFuture[]::new)
                ))
                .thenApply(aVoid -> minIndex());
    }

    // TODO: 用二分查找找到对应的位置，执行压缩
    private void compactPartition(JournalPersistence pp, long minOffset) {

    }



    /**
     * 追加写入StorageEntry
     */
    public long append(MultiplePartitionStorageEntry storageEntry) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(storageEntry.getLength());
        MultiplePartitionStorageEntryParser.serialize(byteBuffer, storageEntry);
        return appendRaw(Collections.singletonList(byteBuffer.array()));
    }

    private void appendPartitionIndices(List<byte[]> storageEntries, long [] offsets) throws IOException{
        for (int i = 0; i < storageEntries.size(); i++) {
            byte[] entryBytes = storageEntries.get(i);
            long offset = offsets[i];
            MultiplePartitionStorageEntry header = MultiplePartitionStorageEntryParser.parseHeader(ByteBuffer.wrap(entryBytes));
            appendPartitionIndex(offset, header);
        }

    }

    private void appendPartitionIndex(long offset, MultiplePartitionStorageEntry header) throws IOException {
        JournalPersistence partitionPersistence = getPartitionPersistence(header.getPartition());
        byte[] bytes = new byte[header.getBatchSize() * INDEX_STORAGE_SIZE];
        ByteBuffer pb = ByteBuffer.wrap(bytes);
        int j = 0;
        while (j ++ < header.getBatchSize()) {
            if(j == 0) {
                pb.putLong(offset);
            } else {
                pb.putLong( -1 * j);
            }
        }
        partitionPersistence.append(bytes);
    }


    private JournalPersistence getPartitionPersistence(short partition) {
        JournalPersistence partitionPersistence = partitionMap.get(partition);
        if(null == partitionPersistence) {
            throw new NosuchPartitionException(partition);
        }
        return partitionPersistence;
    }
    /**
     * 批量追加写入序列化之后的StorageEntry
     */
    public long appendRaw(List<byte []> storageEntries) {
        // 计算索引
        long [] offsets = new long[storageEntries.size()];
        long offset = journalPersistence.max();
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = offset;
            offset += storageEntries.get(i).length;
        }


        try {
            // 写入Journal
            for (byte [] storageEntry : storageEntries) {
                journalPersistence.append(storageEntry);
            }

            // 写入全局索引
            int indexBufferLength = INDEX_STORAGE_SIZE * storageEntries.size();
            ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferLength);

            for (long index: offsets) {
                indexBuffer.putLong(index);
            }
            indexBuffer.flip();
            try {
                indexPersistence.append(indexBuffer.array());
            } catch (TooManyBytesException e) {
                // 如果批量写入超长，改为单条写入
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                for (long index: offsets) {
                    buffer.putLong(0, index);
                    indexPersistence.append(buffer.array());
                }
            }

            // 写入分区索引
            appendPartitionIndices(storageEntries, offsets);

        }  catch (IOException e) {
            throw new JournalException(e);
        }

        return maxIndex();
    }

    /**
     * 给定分区索引位置读取Entry
     * @param partition 分区
     * @param partitionIndex 分区索引
     * @return See {@link BatchEntries}
     */
    public BatchEntries readByPartition(short partition, long partitionIndex) {
        try {
            JournalPersistence pp = getPartitionPersistence(partition);
            long offset = readOffset(pp, partitionIndex);
            long journalOffset;
            short relIndex;
            if(offset < 0) {
                journalOffset = readOffset(pp , partitionIndex + offset);
                relIndex = (short)(-1 * journalOffset);
            } else {
                journalOffset = offset;
                relIndex = (short) 0;
            }


            MultiplePartitionStorageEntry storageEntry = readHeader(journalOffset);

            byte [] entryBytes = journalPersistence
                    .read(
                            journalOffset + MultiplePartitionStorageEntryParser.getHeaderLength(),
                            storageEntry.getLength() - MultiplePartitionStorageEntryParser.getHeaderLength());


            return new BatchEntries(entryBytes, relIndex, storageEntry.getBatchSize());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 给定分区索引位置读取Entry
     * @param partition 分区
     * @param startPartitionIndex 其实索引
     * @param maxSize 最大读取条数
     * @return See {@link BatchEntries} 由于批量Entry不能拆包，返回的Entry数量有可能会大于maxSize。
     */
    public List<BatchEntries> readByPartition(short partition, long startPartitionIndex, int maxSize) {
        List<BatchEntries> list = new LinkedList<>();
        int size = 0;
        long index = startPartitionIndex;
        while (size < maxSize) {
            BatchEntries batchEntries = readByPartition(partition, index);
            int count = batchEntries.getSize() - batchEntries.getOffset();
            size += count ;
            index += count;
            list.add(batchEntries);
        }
        return list;
    }

    /**
     * 给定索引位置读取Entry
     * @return Entry
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public byte [] read(long index){
        checkIndex(index);
        MultiplePartitionStorageEntry storageEntry = readStorageEntry(index);
        return storageEntry.getEntry();
    }

    public MultiplePartitionStorageEntry readStorageEntry(long index){
        checkIndex(index);
        try {
            long offset = readOffset(index);

            MultiplePartitionStorageEntry storageEntry = readHeader(offset);

            byte [] entryBytes = journalPersistence
                    .read(
                            offset + MultiplePartitionStorageEntryParser.getHeaderLength(),
                            storageEntry.getLength() - MultiplePartitionStorageEntryParser.getHeaderLength());
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

            MultiplePartitionStorageEntry storageEntry = readHeader(offset);

            return journalPersistence.read(offset , storageEntry.getLength());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private MultiplePartitionStorageEntry readHeader(long offset) {
        try {
            byte [] headerBytes = journalPersistence.read(offset, MultiplePartitionStorageEntryParser.getHeaderLength());

            return MultiplePartitionStorageEntryParser.parseHeader(ByteBuffer.wrap(headerBytes));
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private long readOffset(long index) {
        return readOffset(indexPersistence, index);
    }
    private long readOffset(JournalPersistence indexPersistence , long index) {
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

        MultiplePartitionStorageEntry storageEntry = readHeader(offset);

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

        List<MultiplePartitionStorageEntry> entries = rawEntries.stream()
                .map(ByteBuffer::wrap)
                .map(MultiplePartitionStorageEntryParser::parseHeader)
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

        long journalOffset = readOffset(index);
        truncatePartitions(journalOffset);
        indexPersistence.truncate(index * INDEX_STORAGE_SIZE);
        journalPersistence.truncate(journalOffset);
    }

    private void truncatePartitions(long journalOffset) throws IOException {
        for (JournalPersistence partitionPerstence : partitionMap.values()) {
            long position = partitionPerstence.max() - INDEX_STORAGE_SIZE;
            while (position > partitionPerstence.min()) {
                long offset = readOffset(partitionPerstence, position / INDEX_STORAGE_SIZE );
                if(offset < journalOffset) {
                    break;
                }
                position -= INDEX_STORAGE_SIZE;
            }
            partitionPerstence.truncate(position <= partitionPerstence.min()? 0L : position + INDEX_STORAGE_SIZE);
        }
    }

    /**
     * 从指定path恢复Journal。
     * 1. 删除journal或者index文件末尾可能存在的不完整的数据。
     * 2. 以Journal为准，修复全局索引和分区索引：删除多余的索引，并创建缺失的索引。
     *
     */
    public void recover(Path path, Properties properties) throws IOException {
        Path journalPath = path.resolve("journal");
        Path indexPath = path.resolve("index");
        Path partitionPath = path.resolve("partitions");
        Files.createDirectories(journalPath);
        Files.createDirectories(indexPath);
        Files.createDirectories(partitionPath);
        journalPersistence.recover(journalPath, replacePropertiesNames(properties, "^persistence\\.journal\\.(.*)$", "$1"));
        // 截掉末尾半条数据
        truncateJournalTailPartialEntry();
        Properties indexProperties = replacePropertiesNames(properties, "^persistence\\.index\\.(.*)$", "$1");
        indexPersistence.recover(indexPath, indexProperties);
        // 截掉末尾半条数据
        indexPersistence.truncate(indexPersistence.max() - indexPersistence.max() % INDEX_STORAGE_SIZE);

        // 删除多余的索引
        truncateExtraIndices();

        // 创建缺失的索引
        buildMissingIndices();

        // 恢复分区索引
        recoverPartitions(partitionPath, indexProperties);

        flush();
        logger.info("Journal recovered, minIndex: {}, maxIndex: {}.", minIndex(), maxIndex());
    }

    private void recoverPartitions(Path partitionPath, Properties properties) throws IOException {

        Short [] partitions = readPartitionDirectories(partitionPath);
        if(null != partitions) {

            Map<Short, Long> lastIndexedOffsetMap = new HashMap<>(partitions.length);

            for (short partition : partitions) {
                JournalPersistence pp = persistenceFactory.createJournalPersistenceInstance();
                pp.recover(partitionPath.resolve(String.valueOf(partition)), properties);
                // 截掉末尾半条数据
                pp.truncate(pp.max() - pp.max() % INDEX_STORAGE_SIZE);
                partitionMap.put(partition, pp);

                truncateTailPartialBatchIndecies(pp);

                lastIndexedOffsetMap.put(partition, getLastIndexedOffset(pp));

            }

            // 重建缺失的分区索引
            long offset = lastIndexedOffsetMap.values().stream().mapToLong(l -> l).min().orElse(journalPersistence.max());
            while (offset < journalPersistence.max()) {
                MultiplePartitionStorageEntry header = readHeader(offset);
                if(offset > lastIndexedOffsetMap.get(header.getPartition())) {
                    appendPartitionIndex(offset, header);
                }
                offset += header.getLength();
            }

        }
    }

    private long getLastIndexedOffset(JournalPersistence pp) {
        // 读出最后一条索引对应的Journal Offset
        long lastIndexedOffset = journalPersistence.min();

        if (pp.max() > 0) {
            long lastIndex = pp.max() / INDEX_STORAGE_SIZE - 1;
            long lastOffset = readOffset(pp, lastIndex);

            if (lastOffset < 0) {
                long firstIndexOfBatchEntries = lastIndex + lastOffset;
                lastIndexedOffset = readOffset(pp, firstIndexOfBatchEntries);
            } else {
                lastIndexedOffset = lastOffset;
            }
        }
        return lastIndexedOffset;
    }

    private void truncateTailPartialBatchIndecies(JournalPersistence pp) throws IOException {
        if(pp.max() > pp.min()) {
            // 如果最后一条索引是批消息的索引，需要检查其完整性
            long lastIndex = pp.max() / INDEX_STORAGE_SIZE - 1;
            long lastOffset = readOffset(pp, lastIndex);
            // 如果是批消息检查这一批消息的索引的完整性，如不完整直接截掉这个批消息的已存储的所有索引
            if (lastOffset < 0) {
                long firstIndexOfBatchEntries = lastIndex + lastOffset;
                long batchEntriesOffset = readOffset(pp, firstIndexOfBatchEntries);
                MultiplePartitionStorageEntry storageEntry = readHeader(batchEntriesOffset);
                if ((lastIndex - 1) * -1 < storageEntry.getBatchSize()) {
                    pp.truncate(firstIndexOfBatchEntries * INDEX_STORAGE_SIZE);
                }
            }
        }
    }

    private Short [] readPartitionDirectories(Path partitionPath) {
        Short [] partitions = null;
        File [] files = partitionPath.toFile()
                .listFiles(file -> file.isDirectory() && file.getName().matches("^\\d+$"));
        if (null != files) {
            partitions = Arrays.stream(files)
                    .map(File::getName)
                    .map(str -> {
                        try {
                            return Short.parseShort(str);
                        } catch (NumberFormatException ignored) {
                            return (short) -1;
                        }
                    })
                    .filter(s -> s >= 0)
                    .toArray(Short[]::new);
        }
        return partitions;
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
            MultiplePartitionStorageEntry storageEntry = readHeader(offset);
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

        long position = journalPersistence.max() - MultiplePartitionStorageEntryParser.getHeaderLength();
        long lastEntryPosition = -1; // 最后连续2条记录中后面那条的位置
        MultiplePartitionStorageEntry lastEntry = null;
        while (position >= journalPersistence.min()) {
            try {
                MultiplePartitionStorageEntry storageEntry = readHeader(position);
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

    private void truncatePartialEntry(long lastEntryPosition, MultiplePartitionStorageEntry lastEntry) throws IOException {
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
            for (JournalPersistence partitionPersistence : partitionMap.values()) {
                partitionPersistence.flush();
            }
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
        for (JournalPersistence persistence : partitionMap.values()) {
            persistence.close();
        }
        indexPersistence.close();
        journalPersistence.close();

    }

}
