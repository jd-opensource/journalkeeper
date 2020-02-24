/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.journal;


import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.exception.JournalException;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.JournalPersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.persistence.TooManyBytesException;
import io.journalkeeper.utils.ThreadSafeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author LiYue
 * Date: 2019-03-15
 */
public class Journal implements RaftJournal, Flushable, Closeable {
    public static final int INDEX_STORAGE_SIZE = Long.BYTES;
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);
    private static final String PARTITION_PATH = "index";
    private static final String INDEX_PATH = "index/all";
    private static final String JOURNAL_PROPERTIES_PATTERN = "^persistence\\.journal\\.(.*)$";
    private static final String INDEX_PROPERTIES_PATTERN = "^persistence\\.index\\.(.*)$";
    private static final Properties DEFAULT_JOURNAL_PROPERTIES = new Properties();
    private static final Properties DEFAULT_INDEX_PROPERTIES = new Properties();

    static {
        DEFAULT_JOURNAL_PROPERTIES.put("file_data_size", String.valueOf(128 * 1024 * 1024));
        DEFAULT_JOURNAL_PROPERTIES.put("cached_file_core_count", String.valueOf(3));
        DEFAULT_JOURNAL_PROPERTIES.put("cached_file_max_count", String.valueOf(10));
        DEFAULT_JOURNAL_PROPERTIES.put("max_dirty_size", String.valueOf(128 * 1024 * 1024));
        DEFAULT_INDEX_PROPERTIES.put("file_data_size", String.valueOf(512 * 1024));
        DEFAULT_INDEX_PROPERTIES.put("cached_file_core_count", String.valueOf(6));
        DEFAULT_INDEX_PROPERTIES.put("cached_file_max_count", String.valueOf(20));
    }

    private final AtomicLong commitIndex = new AtomicLong(0L);
    // 写入index时转换用的缓存
    private final byte[] indexBytes = new byte[INDEX_STORAGE_SIZE];
    private final ByteBuffer indexBuffer = ByteBuffer.wrap(indexBytes);
    // 写入index时转换用的缓存
    private final byte[] commitIndexBytes = new byte[INDEX_STORAGE_SIZE];
    private final ByteBuffer commitIndexBuffer = ByteBuffer.wrap(commitIndexBytes);
    private final JournalPersistence indexPersistence;
    private final JournalPersistence journalPersistence;
    private final Map<Integer, JournalPersistence> partitionMap;
    private final PersistenceFactory persistenceFactory;
    private final BufferPool bufferPool;
    private final JournalEntryParser journalEntryParser;
    private Path basePath = null;
    private Properties indexProperties;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public Journal(PersistenceFactory persistenceFactory, BufferPool bufferPool, JournalEntryParser journalEntryParser) {
        this.indexPersistence = persistenceFactory.createJournalPersistenceInstance();
        this.journalPersistence = persistenceFactory.createJournalPersistenceInstance();
        this.persistenceFactory = persistenceFactory;
        this.journalEntryParser = journalEntryParser;
        this.partitionMap = new ConcurrentHashMap<>();
        this.bufferPool = bufferPool;
    }

    @Override
    public long minIndex() {
        return indexPersistence.min() / INDEX_STORAGE_SIZE;
    }

    @Override
    public long maxIndex() {
        return indexPersistence.max() / INDEX_STORAGE_SIZE;
    }

    @Override
    public long minIndex(int partition) {
        return getPartitionPersistence(partition).min() / INDEX_STORAGE_SIZE;
    }

    @Override
    public long maxIndex(int partition) {
        return getPartitionPersistence(partition).max() / INDEX_STORAGE_SIZE;
    }

    /**
     * 删除给定索引位置之前的数据。
     * 不保证给定位置之前的数据全都被物理删除。
     * 保证给定位置（含）之后的数据不会被删除。
     *
     * @param journalSnapshot 给定最小安全索引位置。
     * @throws IOException 当发生IO异常时抛出
     */
    public void compact(JournalSnapshot journalSnapshot) throws IOException {

        if (journalSnapshot.minIndex() <= minIndex()) {
            return;
        }

        if (journalSnapshot.minIndex() > commitIndex()) {
            throw new IllegalArgumentException(
                    String.format("Given snapshot min index %s should less than commit index %s!",
                            ThreadSafeFormat.formatWithComma(journalSnapshot.minIndex()),
                            ThreadSafeFormat.formatWithComma(commitIndex())
                    )
            );
        }

        // 删除分区索引
        Map<Integer /* partition */ , Long /* min index of the partition */> partitionMinIndices = journalSnapshot.partitionMinIndices();
        synchronized (partitionMap) {


            Iterator<Map.Entry<Integer, JournalPersistence>> iterator = partitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, JournalPersistence> entry = iterator.next();
                int partition = entry.getKey();
                JournalPersistence partitionPersistence = entry.getValue();

                if (partitionMinIndices.containsKey(partition)) {
                    long minPartitionIndices = partitionMinIndices.get(entry.getKey());
                    partitionPersistence.compact(minPartitionIndices * INDEX_STORAGE_SIZE);
                } else {
                    journalPersistence.close();
                    journalPersistence.delete();
                    iterator.remove();
                }

            }

            for (Map.Entry<Integer, Long> entry : partitionMinIndices.entrySet()) {
                int partition = entry.getKey();
                if (!partitionMap.containsKey(partition)) {
                    JournalPersistence partitionPersistence = persistenceFactory.createJournalPersistenceInstance();
                    partitionPersistence.recover(basePath.resolve(PARTITION_PATH).resolve(String.valueOf(partition)),
                            partitionMinIndices.get(partition) * INDEX_STORAGE_SIZE,
                            indexProperties);
                    partitionMap.put(partition, partitionPersistence);
                }
            }
        }

        // 删除全局索引
        indexPersistence.compact(journalSnapshot.minIndex() * INDEX_STORAGE_SIZE);

        // 删除Journal
        journalPersistence.compact(journalSnapshot.minOffset());

    }


    /**
     * 追加写入StorageEntry
     * @param entry 待写入的entry
     * @return entry写入后当前最大全局索引序号
     */
    public long append(JournalEntry entry) {


        // 记录当前最大位置，也是写入的Journal的offset
        long offset = journalPersistence.max();
        byte[] serializedEntry = entry.getSerializedBytes();
        try {
            // 写入Journal header
            journalPersistence.append(serializedEntry);

            // 写入全局索引
            indexBuffer.clear();
            indexBuffer.putLong(offset);
            indexPersistence.append(indexBytes);

        } catch (IOException e) {
            throw new JournalException(e);
        }

        return maxIndex();
    }

    /**
     * 清空整个Journal
     *
     * @param snapshot 清空后Journal的起始位置
     * @throws IOException 发生IO异常时抛出
     */
    public void clear(JournalSnapshot snapshot) throws IOException {
        commitIndex.set(snapshot.minIndex());
        Map<Integer /* partition */ , Long /* min index of the partition */> partitionMinIndices = snapshot.partitionMinIndices();
        synchronized (partitionMap) {


            Iterator<Map.Entry<Integer, JournalPersistence>> iterator = partitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, JournalPersistence> entry = iterator.next();
                int partition = entry.getKey();
                JournalPersistence partitionPersistence = entry.getValue();
                journalPersistence.close();
                journalPersistence.delete();
                iterator.remove();
            }

            for (Map.Entry<Integer, Long> entry : partitionMinIndices.entrySet()) {
                int partition = entry.getKey();
                JournalPersistence partitionPersistence = persistenceFactory.createJournalPersistenceInstance();
                partitionPersistence.recover(basePath.resolve(PARTITION_PATH).resolve(String.valueOf(partition)),
                        partitionMinIndices.get(partition) * INDEX_STORAGE_SIZE,
                        indexProperties);
                partitionMap.put(partition, partitionPersistence);
            }
        }
    }

    /**
     * 批量追加写入StorageEntry
     * @param entries 待写入的批量entry
     * @return 每条entry写入后当前最大全局索引序号
     */
    public List<Long> append(List<JournalEntry> entries) {


        // 记录当前最大位置，也是写入的Journal的offset
        long offset = journalPersistence.max();
        long index = maxIndex();
        ByteBuffer indicesBuffer = ByteBuffer.wrap(new byte[entries.size() * INDEX_STORAGE_SIZE]);
        List<byte[]> entryBuffers = new ArrayList<>(entries.size());

        List<Long> indices = new ArrayList<>(entries.size());
        for (JournalEntry entry : entries) {

            entryBuffers.add(entry.getSerializedBytes());

            indicesBuffer.putLong(offset);
            offset += entry.getLength();
            indices.add(++index);
        }

        try {
            // 写入Journal header
            journalPersistence.append(entryBuffers);

            // 写入全局索引
            indexPersistence.append(indicesBuffer.array());

        } catch (IOException e) {
            throw new JournalException(e);
        }

        return indices;
    }

    // Not thread safe !

    /**
     * 提交journal，已提交的journal不可变。
     * @param index 提交全局索引序号
     * @throws IOException 发生IO异常时抛出
     */
    public void commit(long index) throws IOException {
        long finalCommitIndex;
        while ((finalCommitIndex = commitIndex.get()) < index &&
                finalCommitIndex < maxIndex() &&
                commitIndex.compareAndSet(finalCommitIndex, finalCommitIndex + 1)) {
            long offset = readOffset(finalCommitIndex);

            JournalEntry header = readEntryHeaderByOffset(offset);
            commitIndexBuffer.clear();
            commitIndexBuffer.putLong(offset);
            appendPartitionIndex(commitIndexBytes, header.getPartition(), header.getBatchSize());
        }
    }

    private JournalEntry readEntryHeaderByOffset(long offset) {
        try {
            byte[] headerBytes = journalPersistence.read(offset, journalEntryParser.headerLength());
            return journalEntryParser.parseHeader(headerBytes);
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 获取当前提交的全局索引序号
     * @return 当前提交的全局索引序号
     */
    public long commitIndex() {
        return commitIndex.get();
    }

    private void appendPartitionIndex(byte[] offset, int partition, int batchSize) throws IOException {
        // Create partition which not exists
        if (!partitionMap.containsKey(partition)) {
            addPartition(partition, 0L);
        }
        JournalPersistence partitionPersistence = getPartitionPersistence(partition);
        if (batchSize > 1) {
            byte[] bytes = new byte[batchSize * INDEX_STORAGE_SIZE];
            ByteBuffer pb = ByteBuffer.wrap(bytes);
            int j = 0;
            while (j < batchSize) {
                if (j == 0) {
                    pb.put(offset);
                } else {
                    pb.putLong(-1 * j);
                }
                j++;
            }
            partitionPersistence.append(bytes);
        } else {
            partitionPersistence.append(offset);
        }
    }


    private JournalPersistence getPartitionPersistence(int partition) {
        JournalPersistence partitionPersistence = partitionMap.get(partition);
        if (null == partitionPersistence) {
            throw new NosuchPartitionException(partition);
        }
        return partitionPersistence;
    }

    /**
     * 批量追加写入序列化之后的StorageEntry，用户RAFT复制时，FOLLOWER写入从LEADER复制过来的entries
     * @param storageEntries 从LEADER复制过来的entries
     * @return 写入后的最大全局索引序号
     */
    public long appendBatchRaw(List<byte[]> storageEntries) {
        // 计算索引
        long[] offsets = new long[storageEntries.size()];
        long offset = journalPersistence.max();
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = offset;
            offset += storageEntries.get(i).length;
        }


        try {
            // 写入Journal
            for (byte[] storageEntry : storageEntries) {
                journalPersistence.append(storageEntry);
            }

            // 写入全局索引
            int indexBufferLength = INDEX_STORAGE_SIZE * storageEntries.size();
            ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferLength);

            for (long index : offsets) {
                indexBuffer.putLong(index);
            }
            indexBuffer.flip();
            try {
                indexPersistence.append(indexBuffer.array());
            } catch (TooManyBytesException e) {
                // 如果批量写入超长，改为单条写入
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                for (long index : offsets) {
                    buffer.putLong(0, index);
                    indexPersistence.append(buffer.array());
                }
            }

            // 在commit()提交时才写入分区索引
//            appendPartitionIndices(storageEntries, offsets);

        } catch (IOException e) {
            throw new JournalException(e);
        }

        return maxIndex();
    }

    @Override
    public JournalEntry readByPartition(int partition, long index) {
        JournalPersistence pp = getPartitionPersistence(partition);
        long offset = readOffset(pp, index);
        long journalOffset;
        int relIndex;
        if (offset < 0) {
            journalOffset = readOffset(pp, index + offset);
            relIndex = (int) (-1 * offset);
        } else {
            journalOffset = offset;
            relIndex = 0;
        }

        JournalEntry journal = readByOffset(journalOffset);
        journal.setOffset(relIndex);
        return journal;

    }

    @Override
    public List<JournalEntry> batchReadByPartition(int partition, long startPartitionIndex, int maxSize) {
        List<JournalEntry> list = new LinkedList<>();
        int size = 0;
        long index = startPartitionIndex;
        while (size < maxSize) {
            JournalEntry batchEntry = readByPartition(partition, index);
            int count = batchEntry.getBatchSize() - batchEntry.getOffset();
            size += count;
            index += count;
            list.add(batchEntry);
        }
        return list;
    }


    public JournalEntry read(long index) {
        return journalEntryParser.parse(readRaw(index));
    }

    private JournalEntry readByOffset(long offset) {
        return journalEntryParser.parse(readRawByOffset(offset));
    }

    private byte[] readRaw(long index) {
        readWriteLock.readLock().lock();
        checkIndex(index);
        try {
            long offset = readOffset(index);
            return readRawByOffset(offset);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private byte[] readRawByOffset(long offset) {
        readWriteLock.readLock().lock();
        try {
            int length = readEntryLengthByOffset(offset);
            return journalPersistence
                    .read(offset, length);
        } catch (IOException e) {
            throw new JournalException(e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private int readEntryLengthByOffset(long offset) {
        return readEntryHeaderByOffset(offset).getLength();
    }

    public long readOffset(long index) {
        return readOffset(indexPersistence, index);
    }

    private long readOffset(JournalPersistence indexPersistence, long index) {
        try {
            byte[] indexBytes = indexPersistence.read(index * INDEX_STORAGE_SIZE, INDEX_STORAGE_SIZE);
            return ByteBuffer.wrap(indexBytes).getLong();
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    public List<JournalEntry> batchRead(long index, int size) {
        checkIndex(index);
        List<JournalEntry> list = new ArrayList<>(size);
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
     * @throws IndexUnderflowException 如果 index 小于 minIndex()
     * @throws IndexOverflowException 如果index 不小于 maxIndex()
     */
    public List<byte[]> readRaw(long index, int size) {
        checkIndex(index);
        List<byte[]> list = new ArrayList<>(size);
        long i = index;
        while (list.size() < size && i < maxIndex()) {
            list.add(readRaw(i++));
        }
        return list;
    }

    /**
     * 读取指定索引位置上Entry的Term。
     * @param index 索引位置。
     * @return Term 任期
     * @throws IndexUnderflowException 如果 index 小于 minIndex()
     * @throws IndexOverflowException 如果index 不小于 maxIndex()
     */
    public int getTerm(long index) {
        readWriteLock.readLock().lock();
        try {
            if (index == -1) return -1;
            checkIndex(index);
            long offset = readOffset(index);
            return readEntryHeaderByOffset(offset).getTerm();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }


    /**
     * 从index位置开始：
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目。
     * @param rawEntries 待比较的日志
     * @param startIndex 起始位置
     * @throws IndexUnderflowException 如果 startIndex 小于 minIndex()
     * @throws IndexOverflowException 如果 startIndex 不小于 maxIndex()
     */
    public void compareOrAppendRaw(List<byte[]> rawEntries, long startIndex) {


        List<JournalEntry> entries = rawEntries.stream()
                .map(journalEntryParser::parse)
                .collect(Collectors.toList());
        try {
            long index = startIndex;
            for (int i = 0; i < entries.size(); i++, index++) {
                if (index < maxIndex() && getTerm(index) != entries.get(i).getTerm()) {
                    readWriteLock.writeLock().lock();
                    try {
                        truncate(index);
                    } finally {
                        readWriteLock.writeLock().unlock();
                    }
                }
                if (index == maxIndex()) {
                    appendBatchRaw(rawEntries.subList(i, entries.size()));
                    break;
                }
            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 从索引index位置开始，截掉后面的数据
     * @param index 截取全局索引位置
     * @throws IndexUnderflowException 如果 index < minIndex()
     * @throws IndexOverflowException 如果 index >= maxIndex()
     */
    private void truncate(long index) throws IOException {

        long journalOffset = readOffset(index);
        truncatePartitions(journalOffset);
        indexPersistence.truncate(index * INDEX_STORAGE_SIZE);
        journalPersistence.truncate(journalOffset);

        // 安全更新commitIndex
        long finalCommitIndex;
        while ((finalCommitIndex = commitIndex.get()) > maxIndex() &&
                !commitIndex.compareAndSet(finalCommitIndex, maxIndex())) {
            Thread.yield();
        }

    }

    private void truncatePartitions(long journalOffset) throws IOException {
        for (JournalPersistence partitionPersistence : partitionMap.values()) {
            long position = partitionPersistence.max() - INDEX_STORAGE_SIZE;
            while (position > partitionPersistence.min()) {
                long offset = readOffset(partitionPersistence, position / INDEX_STORAGE_SIZE);
                if (offset < journalOffset) {
                    break;
                }
                position -= INDEX_STORAGE_SIZE;
            }
            partitionPersistence.truncate(position <= partitionPersistence.min() ? 0L : position + INDEX_STORAGE_SIZE);
        }
    }

    /**
     * 从指定path恢复Journal。
     * 1. 删除journal或者index文件末尾可能存在的不完整的数据。
     * 2. 以Journal为准，修复全局索引和分区索引：删除多余的索引，并创建缺失的索引。
     * @param path 恢复目录
     * @param commitIndex 当前journal提交全局索引
     * @param journalSnapshot 快照
     * @param properties 属性
     * @throws IOException 发生IO异常时抛出
     */
    public void recover(Path path, long commitIndex, JournalSnapshot journalSnapshot, Properties properties) throws IOException {
        this.basePath = path;
        Path indexPath = path.resolve(INDEX_PATH);
        Path partitionPath = path.resolve(PARTITION_PATH);
        Properties journalProperties = replacePropertiesNames(properties,
                JOURNAL_PROPERTIES_PATTERN, DEFAULT_JOURNAL_PROPERTIES);
        journalPersistence.recover(path, journalSnapshot.minOffset(), journalProperties);
        // 截掉末尾半条数据
        truncateJournalTailPartialEntry();

        indexProperties = replacePropertiesNames(properties,
                INDEX_PROPERTIES_PATTERN, DEFAULT_INDEX_PROPERTIES);
        indexPersistence.recover(indexPath, journalSnapshot.minIndex() * INDEX_STORAGE_SIZE, indexProperties);
        // 截掉末尾半条数据
        indexPersistence.truncate(indexPersistence.max() - indexPersistence.max() % INDEX_STORAGE_SIZE);

        // 删除多余的索引
        truncateExtraIndices();

        // 创建缺失的索引
        buildMissingIndices();

        checkAndSetCommitIndex(commitIndex);

        // 恢复分区索引
        recoverPartitions(partitionPath, journalSnapshot.partitionMinIndices(), indexProperties);

        flush();
        logger.info("Journal recovered, minIndex: {}, maxIndex: {}, partitions: {}, path: {}.",
                minIndex(), maxIndex(), partitionMap.keySet(), path.toAbsolutePath().toString());
    }

    private void checkAndSetCommitIndex(long commitIndex) {
        this.commitIndex.set(commitIndex);
        if (this.commitIndex.get() < minIndex()) {
            logger.info("Journal commitIndex {} should be not less than minIndex {}, set commitIndex to {}.",
                    this.commitIndex.get(), minIndex(), minIndex());
            this.commitIndex.set(minIndex());
        } else if (this.commitIndex.get() > maxIndex()) {
            logger.info("Journal commitIndex {} should be not greater than maxIndex {}, set commitIndex to {}.",
                    this.commitIndex.get(), maxIndex(), maxIndex());
            this.commitIndex.set(maxIndex());
        }
    }

    private void recoverPartitions(Path partitionPath, Map<Integer, Long> partitionIndices, Properties properties) throws IOException {


        Map<Integer, Long> lastIndexedOffsetMap = new HashMap<>(partitionIndices.size());
        for (Map.Entry<Integer, Long> entry : partitionIndices.entrySet()) {
            int partition = entry.getKey();
            long lastIncludedIndex = entry.getValue();
            JournalPersistence pp = persistenceFactory.createJournalPersistenceInstance();
            pp.recover(partitionPath.resolve(String.valueOf(partition)), lastIncludedIndex * INDEX_STORAGE_SIZE, properties);
            // 截掉末尾半条数据
            pp.truncate(pp.max() - pp.max() % INDEX_STORAGE_SIZE);
            truncateTailPartialBatchIndices(pp);

            partitionMap.put(partition, pp);
            lastIndexedOffsetMap.put(partition, getLastIndexedOffset(pp));
        }

        // 重建缺失的分区索引
        long offset = lastIndexedOffsetMap.values().stream().mapToLong(l -> l).min().orElse(journalPersistence.min());

        // 只创建已提交的分区索引
        long commitOffset = commitIndex.get() == maxIndex() ? journalPersistence.max() :
                readOffset(commitIndex.get());

        // 创建缺失的索引
        while (offset < commitOffset) {
            JournalEntry header = readEntryHeaderByOffset(offset);
            int length = readEntryLengthByOffset(offset);
            if (offset > lastIndexedOffsetMap.get(header.getPartition())) {
                indexBuffer.clear();
                indexBuffer.putLong(offset);
                appendPartitionIndex(indexBytes, header.getPartition(), header.getBatchSize());
            }
            offset += length;
        }

        // 删除未提交部分的分区索引

        for (Integer partition : partitionIndices.keySet()) {
            JournalPersistence partitionPersistence = partitionMap.get(partition);
            long partitionIndex = partitionPersistence.max() / INDEX_STORAGE_SIZE - 1;
            while (partitionIndex * INDEX_STORAGE_SIZE >= partitionPersistence.min()) {
                long journalOffset = readOffset(partitionPersistence, partitionIndex);
                if (journalOffset < commitOffset) {
                    break;
                }
                partitionIndex--;
            }

            partitionIndex += 1;
            partitionPersistence.truncate(partitionIndex * INDEX_STORAGE_SIZE);

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

    private void truncateTailPartialBatchIndices(JournalPersistence pp) throws IOException {
        if (pp.max() > pp.min()) {
            // 如果最后一条索引是批消息的索引，需要检查其完整性
            long lastIndex = pp.max() / INDEX_STORAGE_SIZE - 1;
            long lastOffset = readOffset(pp, lastIndex);
            // 如果是批消息检查这一批消息的索引的完整性，如不完整直接截掉这个批消息的已存储的所有索引
            if (lastOffset < 0) {
                long firstIndexOfBatchEntries = lastIndex + lastOffset;
                long batchEntriesOffset = readOffset(pp, firstIndexOfBatchEntries);
                JournalEntry header = readEntryHeaderByOffset(batchEntriesOffset);
                if ((lastIndex - 1) * -1 < header.getBatchSize()) {
                    pp.truncate(firstIndexOfBatchEntries * INDEX_STORAGE_SIZE);
                }
            }
        }
    }

    private Properties replacePropertiesNames(Properties properties, String fromNameRegex, Properties defaultProperties) {
        Properties jp = new Properties(defaultProperties);
        properties.stringPropertyNames().forEach(k -> {
            String name = k.replaceAll(fromNameRegex, "$1");
            jp.setProperty(name, properties.getProperty(k));
        });
        return jp;
    }

    private void buildMissingIndices() throws IOException {
        // 找到最新一条没有索引位置
        long indexOffset;
        if (indexPersistence.max() - INDEX_STORAGE_SIZE >= indexPersistence.min()) {
            long offset = readOffset(indexPersistence.max() / INDEX_STORAGE_SIZE - 1);
            int length = readEntryLengthByOffset(offset);
            indexOffset = offset + length;
        } else {
            indexOffset = journalPersistence.min();
        }

        // 创建索引
        List<Long> indices = new LinkedList<>();
        while (indexOffset < journalPersistence.max()) {
            indices.add(indexOffset);
            indexOffset += readEntryLengthByOffset(indexOffset);
        }

        // 写入索引
        if (!indices.isEmpty()) {
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
        while ((position -= INDEX_STORAGE_SIZE) >= indexPersistence.min() && readOffset(position / INDEX_STORAGE_SIZE) >= journalPersistence.max()) {
        }
        indexPersistence.truncate(position + INDEX_STORAGE_SIZE);
    }

    private void truncateJournalTailPartialEntry() throws IOException {

        // 找最后的连续2条记录

        long position = journalPersistence.max() - journalEntryParser.headerLength();
        long lastEntryPosition = -1; // 最后连续2条记录中后面那条的位置
        JournalEntry lastEntryHeader = null;
        while (position >= journalPersistence.min()) {
            try {
                JournalEntry header = readByOffset(position);
                // 找到一条记录的开头位置
                if (lastEntryPosition < 0) { // 之前是否已经找到一条？
                    // 这是倒数第一条，记录之
                    lastEntryPosition = position;
                    lastEntryHeader = header;
                    if (lastEntryPosition == journalPersistence.min()) {
                        // 只有一条完整的记录也认为OK，保留之。
                        truncatePartialEntry(lastEntryPosition, lastEntryHeader);
                        return;
                    }
                } else {
                    // 这是倒数第二条
                    if (position + header.getLength() == lastEntryPosition) {
                        // 找到最后2条中位置较小的那条，并且较小那条的位置+长度==较大那条的位置
                        truncatePartialEntry(lastEntryPosition, lastEntryHeader);
                        return;
                    } else { // 之前找到的那个第一条是假的（小概率会出现Entry中恰好也有连续2个字节等于MAGIC）
                        lastEntryPosition = position;
                        lastEntryHeader = header;
                    }
                }
            } catch (Exception ignored) {
            }
            position--;
        }

        // 找到最小位置了，啥也没找到，直接清空所有数据。
        journalPersistence.truncate(journalPersistence.min());
    }

    private void truncatePartialEntry(long lastEntryPosition, JournalEntry lastEntryHeader) throws IOException {
        // 判断最后一条是否完整
        if (lastEntryPosition + lastEntryHeader.getLength() <= journalPersistence.max()) {
            // 完整，截掉后面的部分
            journalPersistence.truncate(lastEntryPosition + lastEntryHeader.getLength());
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
        long flushed;
        do {
            flushed = Stream.concat(Stream.of(journalPersistence, indexPersistence), partitionMap.values().stream())
                    .filter(p -> p.flushed() < p.max())
                    .peek(p -> {
                        try {
                            p.flush();
                        } catch (IOException e) {
                            logger.warn("Flush {} exception: ", p.getBasePath(), e);
                            throw new JournalException(e);
                        }
                    }).count();
        } while (flushed > 0);
    }

    public boolean isDirty() {
        return Stream.concat(Stream.of(journalPersistence, indexPersistence), partitionMap.values().stream())
                .anyMatch(p -> p.flushed() < p.max());
    }

    private void checkIndex(long index) {
        long position = index * INDEX_STORAGE_SIZE;
        if (position < indexPersistence.min()) {
            throw new IndexUnderflowException();
        }
        if (position >= indexPersistence.max()) {
            throw new IndexOverflowException();
        }
    }

    public void rePartition(Set<Integer> partitions) {
        try {
            synchronized (partitionMap) {
                for (int partition : partitions) {
                    if (!partitionMap.containsKey(partition)) {
                        addPartition(partition, 0L);
                    }
                }

                List<Integer> toBeRemoved = new ArrayList<>();
                for (Map.Entry<Integer, JournalPersistence> entry : partitionMap.entrySet()) {
                    if (!partitions.contains(entry.getKey())) {
                        toBeRemoved.add(entry.getKey());
                    }
                }
                for (Integer partition : toBeRemoved) {
                    removePartition(partition);
                }
                logger.info("Journal repartitioned, partitions: {}, path: {}.",
                        partitionMap.keySet(), basePath.toAbsolutePath().toString());

            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }


    @Override
    public long queryIndexByTimestamp(int partition, long timestamp) {
        try {
            if (partitionMap.containsKey(partition)) {
                JournalPersistence indexStore = partitionMap.get(partition);
                long searchedIndex = binarySearchByTimestamp(timestamp, indexStore, indexStore.min() / INDEX_STORAGE_SIZE, indexStore.max() / INDEX_STORAGE_SIZE - 1);

                // 考虑到有可能出现连续n条消息时间相同，找到这n条消息的第一条
                while (searchedIndex - 1 >= indexStore.min() && timestamp <= getStorageTimestamp(indexStore, searchedIndex - 1)) {
                    searchedIndex--;
                }
                return searchedIndex;

            }
        } catch (Throwable e) {
            logger.warn("Query index by timestamp exception: ", e);
        }
        return -1L;
    }

    // 折半查找
    private long binarySearchByTimestamp(long timestamp,
                                         JournalPersistence indexStore,
                                         long leftIndexInclude,
                                         long rightIndexInclude) {

        if (rightIndexInclude <= leftIndexInclude) {
            return -1L;
        }

        if (timestamp <= getStorageTimestamp(indexStore, leftIndexInclude)) {
            return leftIndexInclude;
        }

        if (timestamp >= getStorageTimestamp(indexStore, rightIndexInclude)) {
            return rightIndexInclude;
        }

        if (leftIndexInclude + 1 == rightIndexInclude) {
            return leftIndexInclude;
        }

        long mid = leftIndexInclude + (rightIndexInclude - leftIndexInclude) / 2;

        long midTimestamp = getStorageTimestamp(indexStore, mid);

        if (timestamp < midTimestamp) {
            return binarySearchByTimestamp(timestamp, indexStore, leftIndexInclude, mid);
        } else {
            return binarySearchByTimestamp(timestamp, indexStore, mid, rightIndexInclude);
        }
    }

    private long getStorageTimestamp(
            JournalPersistence indexStore,
            long index) {
        JournalEntry header = readEntryHeaderByOffset(readOffset(indexStore, index));
        return header.getTimestamp();
    }

    @Override
    public Set<Integer> getPartitions() {
        return new HashSet<>(partitionMap.keySet());
    }

    public void addPartition(int partition) throws IOException {
        addPartition(partition, 0L);
    }

    public void addPartition(int partition, long minIndex) throws IOException {
        synchronized (partitionMap) {
            if (!partitionMap.containsKey(partition)) {
                JournalPersistence partitionPersistence = persistenceFactory.createJournalPersistenceInstance();
                partitionPersistence.recover(
                        basePath.resolve(PARTITION_PATH).resolve(String.valueOf(partition)),
                        minIndex * INDEX_STORAGE_SIZE,
                        indexProperties);
                partitionMap.put(partition, partitionPersistence);
            }
        }
    }

    public void removePartition(int partition) throws IOException {
        synchronized (partitionMap) {
            JournalPersistence removedPersistence;
            if ((removedPersistence = partitionMap.remove(partition)) != null) {
                logger.info("Partition removed: {}, journal: {}.", partition, basePath.toAbsolutePath().toString());
                removedPersistence.close();
                removedPersistence.delete();
            }
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

    // For monitor only

    public long flushedIndex() {
        return indexPersistence.flushed() / INDEX_STORAGE_SIZE;
    }

    public JournalPersistence getJournalPersistence() {
        return journalPersistence;
    }

    public JournalPersistence getIndexPersistence() {
        return indexPersistence;
    }

    public Map<Integer, JournalPersistence> getPartitionMap() {
        return Collections.unmodifiableMap(partitionMap);
    }


    public long maxOffset() {
        return journalPersistence.max();
    }

    public Map<Integer, Long> calcPartitionIndices(long journalOffset) {

        Map<Integer, Long> partitionIndices = new HashMap<>(partitionMap.size());
        for (Map.Entry<Integer, JournalPersistence> entry : partitionMap.entrySet()) {
            int partition = entry.getKey();
            JournalPersistence partitionPersistence = entry.getValue();
            long index = maxIndex(partition);
            while (--index >= minIndex(partition)) {
                long offset = readOffset(partitionPersistence, index);
                if (offset < journalOffset) {

                    break;
                }
            }

            partitionIndices.put(partition, index + 1);
        }
        return partitionIndices;
    }

    @Override
    public String toString() {
        return "Journal{" +
                "commitIndex=" + commitIndex +
                ", indexPersistence=" + indexPersistence +
                ", journalPersistence=" + journalPersistence +
                ", partitionMap=" + partitionMap +
                ", basePath=" + basePath +
                '}';
    }
}
