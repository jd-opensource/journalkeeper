package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftEntryHeader;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.entry.Entry;
import com.jd.journalkeeper.core.entry.EntryHeader;
import com.jd.journalkeeper.core.entry.JournalEntryParser;
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
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Journal implements RaftJournal, Flushable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Journal.class);
    private static final String JOURNAL_PATH = "journal";
    private static final String INDEX_PATH = "index";
    private static final String PARTITIONS_PATH = "partitions";
    private final static int INDEX_STORAGE_SIZE = Long.BYTES;
    private final JournalPersistence indexPersistence;
    private final JournalPersistence journalPersistence;
    private final Map<Integer, JournalPersistence> partitionMap;
    private final PersistenceFactory persistenceFactory;
    private final BufferPool bufferPool;
    private Path basePath = null;
    private Properties indexProperties;

    public Journal(PersistenceFactory persistenceFactory, BufferPool bufferPool) {
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
     * 不保证给定位置之前的数据全都被删除。
     * 保证给定位置（含）之后的数据不会被删除。
     */
    public long compact(long givenMinIndex) throws IOException{

        // 首先删除全局索引
        indexPersistence.compact(givenMinIndex * INDEX_STORAGE_SIZE);
        long minIndexOffset = indexPersistence.min();
        // 计算最小全局索引对应的JournalOffset
        long compactJournalOffset = readOffset(minIndexOffset / INDEX_STORAGE_SIZE);
        // 删除分区索引
        for (JournalPersistence partitionPersistence : partitionMap.values()) {
            compactIndices(partitionPersistence, compactJournalOffset);
        }
        // 计算所有索引（全局索引和每个分区索引）对应的JournalOffset的最小值
        compactJournalOffset = Stream.concat(partitionMap.values().stream(), Stream.of(indexPersistence))
                .mapToLong(p -> readOffset(p, p.min() / INDEX_STORAGE_SIZE))
                .min().orElseThrow(() ->new JournalException("Exception on calculate compactJournalOffset"));

        // 删除Journal
        return journalPersistence.compact(compactJournalOffset);

    }

    public void compactByPartition(Map<Integer, Long> compactIndices) throws IOException {
        // 如果给定的compactIndices 中的分区少于当前实际分区，需要用这些分区的minIndex补全分区。
        Map<Integer, Long> compacted = partitionMap.keySet().stream()
                .collect(Collectors.toMap(k -> k, k -> compactIndices.getOrDefault(k,minIndex(k))));
        logger.info("Journal is going to compact: {}, path: {}.", compacted, basePath);

        // 计算Journal的最小安全Offset
        long compactJournalOffset = compacted.entrySet().stream().mapToLong(
                entry -> readOffset(partitionMap.get(entry.getKey()), entry.getValue())
        ).max().orElse(journalPersistence.min());
        logger.info("Safe journal offset: {}. ", compactJournalOffset);

        // 删除分区索引
        for (JournalPersistence indexPersistence : partitionMap.values()) {
            compactIndices(indexPersistence, compactJournalOffset);
        }
        // 删除全局索引
        compactIndices(indexPersistence, compactJournalOffset);
    }

    private void compactIndices(JournalPersistence pp, long minJournalOffset) throws IOException{
        long indexOffset = binarySearchFloorOffset(pp, minJournalOffset, pp.min(), pp.max());
        pp.compact(indexOffset);
    }

    /**
     * 使用二分法递归查找一个索引在indexPersistence位置P：
     *
     * 1. P >= minIndexOffset && p < maxIndexOffset;
     * 2. P.offset <= targetJournalOffset
     * 3. P为所有满足条件1、2的位置中的最大值
     * 4. 如果 targetJournalOffset <= minIndexOffset.offset 返回 minIndexOffset
     * 5. 如果 targetJournalOffset >= maxIndexOffset.offset 返回 maxIndexOffset
     *
     * @param indexPersistence 索引存储
     * @param targetJournalOffset 目标索引内的Journal offset
     * @param minIndexOffset 最小查找索引存储位置（含）
     * @param maxIndexOffset 最大查找索引存储位置（不含）
     * @return 符合条件的索引位置的最大值
     */
    private long binarySearchFloorOffset (
            JournalPersistence indexPersistence, long targetJournalOffset,
            long minIndexOffset, long maxIndexOffset) throws IOException {
        long minOffset = readOffset(indexPersistence, minIndexOffset / INDEX_STORAGE_SIZE);
        long maxOffset = indexPersistence.max() == maxIndexOffset ? Long.MAX_VALUE :
                readOffset(indexPersistence , maxIndexOffset / INDEX_STORAGE_SIZE);


        if (targetJournalOffset <= minOffset) {
            return minIndexOffset;
        }
        if(targetJournalOffset >= maxOffset) {
            return maxIndexOffset;
        }
        if(minIndexOffset + INDEX_STORAGE_SIZE >= maxIndexOffset) {
            return maxIndexOffset;
        }

        // 折半并取整
        long midIndexOffset = minIndexOffset +  (maxIndexOffset - minIndexOffset) / 2;
        midIndexOffset = midIndexOffset - midIndexOffset % INDEX_STORAGE_SIZE;

        long midOffset = readOffset(indexPersistence, midIndexOffset / INDEX_STORAGE_SIZE);

        if(targetJournalOffset < midOffset) {
            return binarySearchFloorOffset(indexPersistence, targetJournalOffset, minIndexOffset, midIndexOffset);
        } else {
            return binarySearchFloorOffset(indexPersistence, targetJournalOffset, midIndexOffset, maxIndexOffset);
        }

    }


    /**
     * 追加写入StorageEntry
     */
    public long append(Entry storageEntry) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(storageEntry.getHeader().getPayloadLength() + JournalEntryParser.getHeaderLength());
        JournalEntryParser.serialize(byteBuffer, storageEntry);
        return appendRaw(Collections.singletonList(byteBuffer.array()));
    }

    private void appendPartitionIndices(List<byte[]> storageEntries, long [] offsets) throws IOException{
        for (int i = 0; i < storageEntries.size(); i++) {
            byte[] entryBytes = storageEntries.get(i);
            long offset = offsets[i];
            EntryHeader header = JournalEntryParser.parseHeader(ByteBuffer.wrap(entryBytes));
            appendPartitionIndex(offset, header);
        }

    }

    private void appendPartitionIndex(long offset, EntryHeader header) throws IOException {
        JournalPersistence partitionPersistence = getPartitionPersistence(header.getPartition());
        byte[] bytes = new byte[header.getBatchSize() * INDEX_STORAGE_SIZE];
        ByteBuffer pb = ByteBuffer.wrap(bytes);
        int j = 0;
        while (j < header.getBatchSize()) {
            if(j == 0) {
                pb.putLong(offset);
            } else {
                pb.putLong( -1 * j);
            }
            j ++;
        }
        partitionPersistence.append(bytes);
    }


    private JournalPersistence getPartitionPersistence(int partition) {
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
     * @param index 分区索引
     * @return See {@link RaftEntry}
     */
    @Override
    public RaftEntry readByPartition(int partition, long index) {
        try {
            JournalPersistence pp = getPartitionPersistence(partition);
            long offset = readOffset(pp, index);
            long journalOffset;
            int relIndex;
            if(offset < 0) {
                journalOffset = readOffset(pp , index + offset);
                relIndex = (int)(-1 * offset);
            } else {
                journalOffset = offset;
                relIndex = (int) 0;
            }


            EntryHeader header = readEntryHeaderByOffset(journalOffset);

            byte [] entryBytes = journalPersistence
                    .read(
                            journalOffset + JournalEntryParser.getHeaderLength(),
                            header.getPayloadLength());

            header.setOffset(relIndex);
            return new Entry(header, entryBytes);
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    /**
     * 给定分区索引位置读取Entry
     * @param partition 分区
     * @param startPartitionIndex 其实索引
     * @param maxSize 最大读取条数
     * @return See {@link Entry} 由于批量Entry不能拆包，返回的Entry数量有可能会大于maxSize。
     */
    @Override
    public List<RaftEntry> readByPartition(int partition, long startPartitionIndex, int maxSize) {
        List<RaftEntry> list = new LinkedList<>();
        int size = 0;
        long index = startPartitionIndex;
        while (size < maxSize) {
            RaftEntry batchEntries = readByPartition(partition, index);
            int count = batchEntries.getHeader().getBatchSize() - batchEntries.getHeader().getOffset();
            size += count ;
            index += count;
            list.add(batchEntries);
        }
        // TODO: 返回的数量不正确
        return list;
    }


    /**
     * 给定索引位置读取Entry
     * @return Entry
     * @throws IndexUnderflowException 如果 index< minIndex()
     * @throws IndexOverflowException 如果index >= maxIndex()
     */
    public Entry read(long index){
        checkIndex(index);
        try {
            long offset = readOffset(index);

            EntryHeader header = readEntryHeaderByOffset(offset);

            byte [] entry = journalPersistence
                    .read(
                            offset + JournalEntryParser.getHeaderLength(),
                            header.getPayloadLength());


            return new Entry(header, entry);
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private byte [] readRawStorageEntry(long index){
        checkIndex(index);
        try {
            long offset = readOffset(index);

            EntryHeader header = readEntryHeaderByOffset(offset);

            return journalPersistence.read(offset , header.getPayloadLength() + JournalEntryParser.getHeaderLength());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    private EntryHeader readEntryHeaderByOffset(long offset) {
        try {
            byte [] headerBytes = journalPersistence.read(offset, JournalEntryParser.getHeaderLength());

            return JournalEntryParser.parseHeader(ByteBuffer.wrap(headerBytes));
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
    public List<RaftEntry> batchRead(long index, int size) {
        checkIndex(index);
        List<RaftEntry> list = new ArrayList<>(size);
        long i = index;
        while (list.size() < size && i < maxIndex()) {
            list.add(read(i++));
        }
        return list;
    }

    @Override
    public RaftEntryHeader readEntryHeader(long index) {
        return readEntryHeaderByOffset(readOffset(index));
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
        return readEntryHeaderByOffset(offset).getTerm();
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


        List<EntryHeader> entries = rawEntries.stream()
                .map(ByteBuffer::wrap)
                .map(JournalEntryParser::parseHeader)
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
        for (JournalPersistence partitionPersistence : partitionMap.values()) {
            long position = partitionPersistence.max() - INDEX_STORAGE_SIZE;
            while (position > partitionPersistence.min()) {
                long offset = readOffset(partitionPersistence, position / INDEX_STORAGE_SIZE );
                if(offset < journalOffset) {
                    break;
                }
                position -= INDEX_STORAGE_SIZE;
            }
            partitionPersistence.truncate(position <= partitionPersistence.min()? 0L : position + INDEX_STORAGE_SIZE);
        }
    }

    /**
     * 从指定path恢复Journal。
     * 1. 删除journal或者index文件末尾可能存在的不完整的数据。
     * 2. 以Journal为准，修复全局索引和分区索引：删除多余的索引，并创建缺失的索引。
     *
     */
    public void recover(Path path, Properties properties) throws IOException {
        this.basePath = path;
        Path journalPath = path.resolve(JOURNAL_PATH);
        Path indexPath = path.resolve(INDEX_PATH);
        Path partitionPath = path.resolve(PARTITIONS_PATH);
        journalPersistence.recover(journalPath, replacePropertiesNames(properties, "^persistence\\.journal\\.(.*)$", "$1"));
        // 截掉末尾半条数据
        truncateJournalTailPartialEntry();
        indexProperties = replacePropertiesNames(properties, "^persistence\\.index\\.(.*)$", "$1");
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

        Integer [] partitions = readPartitionDirectories(partitionPath);
        Map<Integer, Long> lastIndexedOffsetMap = new HashMap<>(partitions.length);
        for (int partition : partitions) {
            JournalPersistence pp = persistenceFactory.createJournalPersistenceInstance();
            pp.recover(partitionPath.resolve(String.valueOf(partition)), properties);
            // 截掉末尾半条数据
            pp.truncate(pp.max() - pp.max() % INDEX_STORAGE_SIZE);
            partitionMap.put(partition, pp);

            truncateTailPartialBatchIndices(pp);

            lastIndexedOffsetMap.put(partition, getLastIndexedOffset(pp));

        }

        // 重建缺失的分区索引
        long offset = lastIndexedOffsetMap.values().stream().mapToLong(l -> l).min().orElse(journalPersistence.max());
        while (offset < journalPersistence.max()) {
            EntryHeader header = readEntryHeaderByOffset(offset);
            if(offset > lastIndexedOffsetMap.get(header.getPartition())) {
                appendPartitionIndex(offset, header);
            }
            offset += header.getPayloadLength() + JournalEntryParser.getHeaderLength();
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
        if(pp.max() > pp.min()) {
            // 如果最后一条索引是批消息的索引，需要检查其完整性
            long lastIndex = pp.max() / INDEX_STORAGE_SIZE - 1;
            long lastOffset = readOffset(pp, lastIndex);
            // 如果是批消息检查这一批消息的索引的完整性，如不完整直接截掉这个批消息的已存储的所有索引
            if (lastOffset < 0) {
                long firstIndexOfBatchEntries = lastIndex + lastOffset;
                long batchEntriesOffset = readOffset(pp, firstIndexOfBatchEntries);
                EntryHeader header = readEntryHeaderByOffset(batchEntriesOffset);
                if ((lastIndex - 1) * -1 < header.getBatchSize()) {
                    pp.truncate(firstIndexOfBatchEntries * INDEX_STORAGE_SIZE);
                }
            }
        }
    }

    private Integer [] readPartitionDirectories(Path partitionPath) throws IOException {
        return Files.isDirectory(partitionPath) ?
                Files.list(partitionPath)
                .filter(Files::isDirectory)
                .map(path -> path.getFileName().toString())
                .filter(filename -> filename.matches("^\\d+$"))
                .map(str -> {
                    try {
                        return Integer.parseInt(str);
                    } catch (NumberFormatException ignored) {
                        return -1;
                    }
                })
                .filter(s -> s >= 0)
                .toArray(Integer[]::new):
                new Integer[0];
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
            EntryHeader header = readEntryHeaderByOffset(offset);
            indexOffset = offset + header.getPayloadLength() + JournalEntryParser.getHeaderLength();
        } else {
            indexOffset = journalPersistence.min();
        }

        // 创建索引
        List<Long> indices = new LinkedList<>();
        while (indexOffset < journalPersistence.max()) {
            indices.add(indexOffset);
            indexOffset += readEntryHeaderByOffset(indexOffset).getPayloadLength() + JournalEntryParser.getHeaderLength();
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

        long position = journalPersistence.max() - JournalEntryParser.getHeaderLength();
        long lastEntryPosition = -1; // 最后连续2条记录中后面那条的位置
        EntryHeader lastEntryHeader = null;
        while (position >= journalPersistence.min()) {
            try {
                EntryHeader header = readEntryHeaderByOffset(position);
                // 找到一条记录的开头位置
                if(lastEntryPosition < 0) { // 之前是否已经找到一条？
                    // 这是倒数第一条，记录之
                    lastEntryPosition = position;
                    lastEntryHeader = header;
                    if(lastEntryPosition == journalPersistence.min()) {
                        // 只有一条完整的记录也认为OK，保留之。
                        truncatePartialEntry(lastEntryPosition, lastEntryHeader);
                        return;
                    }
                } else {
                    // 这是倒数第二条
                    if(position + header.getPayloadLength() + JournalEntryParser.getHeaderLength() == lastEntryPosition) {
                        // 找到最后2条中位置较小的那条，并且较小那条的位置+长度==较大那条的位置
                        truncatePartialEntry(lastEntryPosition, lastEntryHeader);
                        return;
                    } else { // 之前找到的那个第一条是假的（小概率会出现Entry中恰好也有连续2个字节等于MAGIC）
                        lastEntryPosition = position;
                        lastEntryHeader = header;
                    }
                }
            } catch (Exception ignored) {}
            position --;
        }

        // 找到最小位置了，啥也没找到，直接清空所有数据。
        journalPersistence.truncate(journalPersistence.min());
    }

    private void truncatePartialEntry(long lastEntryPosition, EntryHeader lastEntryHeader) throws IOException {
        // 判断最后一条是否完整
        if(lastEntryPosition + lastEntryHeader.getPayloadLength() + JournalEntryParser.getHeaderLength() <= journalPersistence.max()) {
            // 完整，截掉后面的部分
            journalPersistence.truncate(lastEntryPosition + lastEntryHeader.getPayloadLength() + JournalEntryParser.getHeaderLength());
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

    public void rePartition(Set<Integer> partitions) {
        try {
            synchronized (partitionMap) {
                for (int partition : partitions) {
                    if (!partitionMap.containsKey(partition)) {
                        addPartition(partition);
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

            }
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    @Override
    public Set<Integer> getPartitions() {
        return partitionMap.keySet();
    }

    public void addPartition(int partition) throws IOException {
        synchronized (partitionMap) {
            if (!partitionMap.containsKey(partition)) {
                JournalPersistence partitionPersistence = persistenceFactory.createJournalPersistenceInstance();
                partitionPersistence.recover(basePath.resolve(PARTITIONS_PATH).resolve(String.valueOf(partition)), indexProperties);
                partitionMap.put(partition, partitionPersistence);
            }
        }
    }

    public void removePartition(int partition) throws IOException {
        synchronized (partitionMap) {
            JournalPersistence removedPersistence;
            if ((removedPersistence = partitionMap.remove(partition)) != null) {
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

}
