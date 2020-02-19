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
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricSupport;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.test.ByteUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author LiYue
 * Date: 2019-04-03
 */
public class JournalTest {
    private static final Logger logger = LoggerFactory.getLogger(JournalTest.class);
    private Path path = null;
    private Journal journal = null;
    private Set<Integer> partitions = Stream.of(0, 4, 5, 6).collect(Collectors.toSet());
    private JournalEntryParser journalEntryParser = new DefaultJournalEntryParser();

    private static File findLastFile(Path parent) {

        return Arrays.stream(Objects.requireNonNull(parent.toFile()
                .listFiles(file -> file.isFile() && file.getName().matches("\\d+"))))
                .max(Comparator.comparingLong(f -> Long.parseLong(f.getName())))
                .orElse(null);

    }

    @Before
    public void before() throws IOException, InterruptedException {
        path = TestPathUtils.prepareBaseDir();
        journal = createJournal();
    }

    @Ignore
    @Test
    public void writeRawPerformanceTest() {
        // 每条entry大小
        int entrySize = 1024;
        // 每批多少条
        int batchCount = 1024;
        int term = 8;
        int partition = 6;
        // 循环写入多少批
        int loopCount = 10 * 1024;

        List<byte[]> entries = ByteUtils.createFixedSizeByteList(entrySize, batchCount);
        // 构建测试的entry
        List<byte[]> rawEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .map(this::serialize)
                        .collect(Collectors.toList());

        // 启动刷盘线程
        AsyncLoopThread flushJournalThread = ThreadBuilder.builder()
                .name("FlushJournalThread")
                .doWork(journal::flush)
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("FlushJournalThread Exception: ", e))
                .daemon(true)
                .build();
        // 启动提交线程
        AsyncLoopThread commitJournalThread = ThreadBuilder.builder()
                .name("CommitJournalThread")
                .doWork(() -> journal.commit(journal.maxIndex()))
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("CommitJournalThread Exception: ", e))
                .daemon(true)
                .build();


        flushJournalThread.start();
        commitJournalThread.start();
        try {
            long t0 = System.currentTimeMillis();
            for (int i = 0; i < loopCount; i++) {
                for (byte[] rawEntry : rawEntries) {
                    journal.appendBatchRaw(Collections.singletonList(rawEntry));
                }
            }
            long t1 = System.currentTimeMillis();
            while (journal.isDirty()) {
                Thread.yield();
            }
            long t2 = System.currentTimeMillis();
            long writeTakes = t1 - t0;
            long flushTakes = t2 - t0;
            long totalTraffic = (long) loopCount * batchCount * entrySize;
            long totalCount = loopCount * batchCount;

            logger.info("Total {}, write tps: {}/s, traffic: {}/s, flush tps: {}/s, traffic: {}/s.",
                    Format.formatSize(totalTraffic),
                    Format.formatWithComma(totalCount * 1000L / writeTakes),
                    Format.formatSize(totalTraffic * 1000L / writeTakes),
                    Format.formatWithComma(totalCount * 1000L / flushTakes),
                    Format.formatSize(totalTraffic * 1000L / flushTakes)
            );
        } finally {
            flushJournalThread.stop();
            commitJournalThread.stop();
        }
    }

    @Ignore
    @Test
    public void writePerformanceTest() {
        // 每条entry大小
        int entrySize = 1024;
        // 每批多少条
        int batchCount = 1024;
        int term = 8;
        int partition = 6;
        // 循环写入多少批
        int loopCount = 10 * 1024;

        List<byte[]> entries = ByteUtils.createFixedSizeByteList(entrySize, batchCount);
        // 构建测试的entry
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .collect(Collectors.toList());

        // 启动刷盘线程
        AsyncLoopThread flushJournalThread = ThreadBuilder.builder()
                .name("FlushJournalThread")
                .doWork(journal::flush)
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("FlushJournalThread Exception: ", e))
                .daemon(true)
                .build();

        // 启动提交线程
        AsyncLoopThread commitJournalThread = ThreadBuilder.builder()
                .name("CommitJournalThread")
                .doWork(() -> journal.commit(journal.maxIndex()))
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("CommitJournalThread Exception: ", e))
                .daemon(true)
                .build();


        flushJournalThread.start();
        commitJournalThread.start();
        try {
            long t0 = System.currentTimeMillis();

            for (int i = 0; i < loopCount; i++) {
                for (JournalEntry storageEntry : storageEntries) {
                    journal.append(storageEntry);
                }
            }
            long t1 = System.currentTimeMillis();
            while (journal.isDirty()) {
                Thread.yield();
            }
            long t2 = System.currentTimeMillis();
            long writeTakes = t1 - t0;
            long flushTakes = t2 - t0;
            long totalTraffic = (long) loopCount * batchCount * entrySize;
            long totalCount = loopCount * batchCount;

            logger.info("Total {}, write tps: {}/s, traffic: {}/s, flush tps: {}/s, traffic: {}/s.",
                    Format.formatSize(totalTraffic),
                    Format.formatWithComma(totalCount * 1000L / writeTakes),
                    Format.formatSize(totalTraffic * 1000L / writeTakes),
                    Format.formatWithComma(totalCount * 1000L / flushTakes),
                    Format.formatSize(totalTraffic * 1000L / flushTakes));
        } finally {
            flushJournalThread.stop();
            commitJournalThread.stop();
        }
    }

    @Ignore
    @Test
    public void queuedWritePerformanceTest() throws InterruptedException {
        // 每条entry大小
        int entrySize = 1024;
        // 每批多少条
        int batchCount = 1024;
        int term = 8;
        int partition = 6;
        // 循环写入多少批
        int loopCount = 10 * 1024;
        List<byte[]> entries = ByteUtils.createFixedSizeByteList(entrySize, batchCount);
        // 构建测试的entry
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .collect(Collectors.toList());

        BlockingQueue<JournalEntry> entryQueue = new LinkedBlockingQueue<>(1024);
        // 启动刷盘线程
        AsyncLoopThread flushJournalThread = ThreadBuilder.builder()
                .name("FlushJournalThread")
                .doWork(journal::flush)
                .sleepTime(10L, 10L)
                .onException(e -> logger.warn("FlushJournalThread Exception: ", e))
                .daemon(true)
                .build();
        // 启动提交线程
        AsyncLoopThread commitJournalThread = ThreadBuilder.builder()
                .name("CommitJournalThread")
                .doWork(() -> journal.commit(journal.maxIndex()))
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("CommitJournalThread Exception: ", e))
                .daemon(true)
                .build();

        AsyncLoopThread appendJournalThread = ThreadBuilder.builder()
                .name("AppendJournalThread")
                .doWork(() -> journal.append(entryQueue.take()))
                .sleepTime(0L, 0L)
                .onException(e -> logger.warn("AppendJournalThread Exception: ", e))
                .daemon(true)
                .build();

        flushJournalThread.start();
        commitJournalThread.start();
        appendJournalThread.start();

        JMetricFactory factory = ServiceSupport.load(JMetricFactory.class);
        JMetric metric = factory.create("WRITE");

        try {
            long t0 = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                for (JournalEntry storageEntry : storageEntries) {
                    metric.start();
                    entryQueue.put(storageEntry);
                    metric.end(storageEntry.getLength());
                }
            }
            logger.info("{}", JMetricSupport.formatNs(metric.get()));
            while (!entryQueue.isEmpty()) {
                Thread.yield();
            }
            long t1 = System.nanoTime();

            while (journal.isDirty()) {
                Thread.yield();
            }
            long t2 = System.nanoTime();

            long takeMs = (t1 - t0) / 1000000;
            long totalSize = (long) loopCount * batchCount * entrySize;

            logger.info("Write {} take {} ms, {}/s.",
                    Format.formatSize(totalSize),
                    takeMs,
                    Format.formatSize(totalSize * 1000 / takeMs));


            takeMs = (t2 - t0) / 1000000;

            logger.info("Flush {} take {} ms, {}/s.",
                    Format.formatSize(totalSize),
                    takeMs,
                    Format.formatSize(totalSize * 1000 / takeMs));
        } finally {
            appendJournalThread.stop();
            commitJournalThread.stop();
            flushJournalThread.stop();
        }
    }

    @Test
    public void writeReadEntryTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        int partition = 6;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .collect(Collectors.toList());
        long maxIndex = 0L;
        for (JournalEntry storageEntry : storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }

        index = journal.minIndex();

        while (index < journal.maxIndex()) {
            JournalEntry readStorageEntry = journal.read(index);
            JournalEntry writeStorageEntry = storageEntries.get((int) index);
            Assert.assertEquals(writeStorageEntry, readStorageEntry);
            index++;
        }

    }

    @Test
    public void batchWriteTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        int partition = 6;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .collect(Collectors.toList());
        List<Long> indices = journal.append(storageEntries);
        Assert.assertEquals(size, indices.size());
        for (int i = 0; i < indices.size(); i++) {
            Assert.assertEquals(i + 1, (long) indices.get(i));
        }
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }

        index = journal.minIndex();

        while (index < journal.maxIndex()) {
            JournalEntry readStorageEntry = journal.read(index);
            JournalEntry writeStorageEntry = storageEntries.get((int) index);
            Assert.assertEquals(writeStorageEntry, readStorageEntry);
            index++;
        }

    }

    @Test
    public void readByPartitionTest() throws IOException, InterruptedException {
        int maxLength = 1024;
        int size = 1024;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .collect(Collectors.toList());

        Map<Integer, List<JournalEntry>> partitionEntries =
                partitions.stream().collect(Collectors.toMap(p -> p, p -> new ArrayList<>()));
        List<Integer> partitionList = new ArrayList<>(partitions);
        for (int i = 0; i < storageEntries.size(); i++) {
            int partition = partitionList.get(i % partitions.size());
            JournalEntry entry = storageEntries.get(i);
            entry.setPartition(partition);
            partitionEntries.get(partition).add(entry);
        }


        for (JournalEntry storageEntry : storageEntries) {
            journal.append(storageEntry);
        }
        journal.commit(journal.maxIndex());
        long commitIndex = journal.commitIndex();
        journal.flush();
        journal.close();

        journal = createJournal(commitIndex);
        for (Map.Entry<Integer, List<JournalEntry>> entry : partitionEntries.entrySet()) {
            Integer partition = entry.getKey();
            List<JournalEntry> pEntries = entry.getValue();
            Assert.assertEquals(pEntries.size(), journal.maxIndex(partition));
            Assert.assertEquals(0, journal.minIndex(partition));
            for (int i = 0; i < pEntries.size(); i++) {
                JournalEntry pEntry = pEntries.get(i);
                JournalEntry batchEntries = journal.readByPartition(partition, i);
                Assert.assertEquals(pEntry.getBatchSize(), batchEntries.getBatchSize());
                Assert.assertEquals(0, batchEntries.getOffset());
                Assert.assertEquals(pEntry.getPayload(), batchEntries.getPayload());
            }
        }
    }

    @Test
    public void batchEntriesTest() throws IOException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        int batchSize = 23;
        int partition = 0;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(partition))
                        .peek(entry -> entry.setBatchSize(batchSize))
                        .collect(Collectors.toList());


        for (JournalEntry storageEntry : storageEntries) {
            journal.append(storageEntry);
        }
        journal.commit(journal.maxIndex());
        Assert.assertEquals(size * batchSize, journal.maxIndex(partition));

        for (int i = 0; i < journal.maxIndex(); i++) {
            JournalEntry batchEntries = journal.readByPartition(partition, i);
            Assert.assertEquals(i % batchSize, batchEntries.getOffset());
            Assert.assertEquals(storageEntries.get(i / batchSize).getPayload(), batchEntries.getPayload());
            Assert.assertEquals(batchSize, batchEntries.getBatchSize());
        }
    }

    @Test
    public void writeReadRawTest() throws IOException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<byte[]> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendBatchRaw(storageEntries);
        journal.commit(journal.maxIndex());
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        List<byte[]> readStorageEntries = journal.readRaw(index, (int) (maxIndex - index));
        Assert.assertEquals(storageEntries.size(), readStorageEntries.size());
        IntStream.range(0, readStorageEntries.size())
                .forEach(i -> Assert.assertArrayEquals(storageEntries.get(i), readStorageEntries.get(i)));

    }

    private byte[] serialize(JournalEntry storageEntry) {
        return storageEntry.getSerializedBytes();
    }

    @Test
    public void compareOrAppendTest() {
        //已有的日志Terms：8, 8, 8, 8, 9, 9 , 9 , 10, 10, 10
        //新增的日志Terms：      8, 8, 9, 10, 11, 11
        //结果：          8, 8, 8, 8, 9, 10, 11, 11
        int maxLength = 128;
        int[] terms = new int[]{8, 8, 8, 8, 9, 9, 9, 10, 10, 10};
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, terms.length);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(0))
                        .peek(entry -> entry.setPartition(0))
                        .collect(Collectors.toList());
        for (int i = 0; i < terms.length; i++) {
            storageEntries.get(i).setTerm(terms[i]);
        }

        long maxIndex = 0L;
        for (JournalEntry storageEntry : storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(terms.length, maxIndex);
        Assert.assertEquals(terms.length, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        int[] appendTerms = new int[]{8, 8, 9, 10, 11, 11};
        List<byte[]> appendEntries = ByteUtils.createRandomSizeByteList(maxLength, appendTerms.length);
        List<JournalEntry> appendStorageEntries =
                appendEntries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(0))
                        .peek(entry -> entry.setPartition(0))
                        .collect(Collectors.toList());
        for (int i = 0; i < appendTerms.length; i++) {
            appendStorageEntries.get(i).setTerm(appendTerms[i]);
        }

        List<byte[]> appendRawStorageEntries = appendStorageEntries.stream()
                .map(this::serialize).collect(Collectors.toList());
        int startIndex = 2;
        journal.compareOrAppendRaw(appendRawStorageEntries, startIndex);

        Assert.assertEquals(8, journal.maxIndex());
        int[] expectedTerms = new int[]{8, 8, 8, 8, 9, 10, 11, 11};
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(expectedTerms[i], journal.getTerm(i));
            if (i < 5) {
                Assert.assertEquals(storageEntries.get(i).getPayload(), journal.read(i).getPayload());
            } else {
                Assert.assertEquals(appendStorageEntries.get(i - startIndex).getPayload(), journal.read(i).getPayload());
            }
        }
    }

    @Test
    public void flushRecoverTest() throws IOException, InterruptedException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte[]> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<byte[]> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendBatchRaw(storageEntries);
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        journal.flush();
        journal.close();

        journal = createJournal();
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }

    }

    @Test
    public void recoverTest() throws IOException, InterruptedException {


        int entrySize = 128;
        int size = 15;
        int entriesPerFile = 5;
        int term = 8;

        journal.close();
        Properties properties = new Properties();
        properties.setProperty("persistence.journal.file_data_size", String.valueOf((entrySize + journalEntryParser.headerLength()) * entriesPerFile));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(Long.BYTES * entriesPerFile));
        journal = createJournal(properties);


        List<byte[]> entries = ByteUtils.createFixedSizeByteList(entrySize, size);
        List<byte[]> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendBatchRaw(storageEntries);
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        journal.flush();
        journal.close();

        // 测试多余数据是否能自动删除

        File lastFile = findLastFile(path);
//        FileUtils.write(lastFile, "Extra bytes", StandardCharsets.UTF_8, true);

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }
        journal.flush();
        journal.close();


        // 删掉最后一条Entry的部分字节，测试是否能自动删除多余的半条Entry和索引
        lastFile = findLastFile(path);
        try (RandomAccessFile raf = new RandomAccessFile(lastFile, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - entrySize / 3);
        }
        size--;

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }
        journal.flush();
        journal.close();

        // 删掉索引最后的15个字节（一条完整索引+一条不完整索引），测试自动重建索引
        lastFile = findLastFile(path.resolve("index").resolve("all"));
        try (RandomAccessFile raf = new RandomAccessFile(lastFile, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - Long.BYTES * 2 - 1);
        }

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int) index), journal.read(index).getPayload().getBytes());
            index++;
        }

    }

    @Test
    public void compactTest() throws Exception {
        int entrySize = 128;
        int size = 15;
        int entriesPerFile = 5;
        int term = 8;

        journal.close();
        Properties properties = new Properties();
        properties.setProperty("persistence.journal.file_data_size", String.valueOf((entrySize + journalEntryParser.headerLength()) * entriesPerFile));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(Long.BYTES * entriesPerFile));
        journal = createJournal(properties);


        List<byte[]> entries = ByteUtils.createFixedSizeByteList(entrySize, size);
        List<JournalEntry> storageEntries =
                entries.stream()
                        .map(entry -> journalEntryParser.createJournalEntry(entry))
                        .peek(entry -> entry.setTerm(term))
                        .peek(entry -> entry.setPartition(0))
                        .collect(Collectors.toList());
        List<Integer> partitionList = new ArrayList<>(partitions);
        for (int i = 0; i < storageEntries.size(); i++) {
            int partition = partitionList.get(i % partitions.size());
            JournalEntry entry = storageEntries.get(i);
            entry.setPartition(partition);
        }

        long maxIndex = 0L;
        List<JournalSnapshotImpl> snapshotList = new ArrayList<>(storageEntries.size());
        for (JournalEntry storageEntry : storageEntries) {
            snapshotList.add(new JournalSnapshotImpl(journal));
            maxIndex = journal.append(storageEntry);
            journal.commit(journal.maxIndex());
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        journal.commit(journal.maxIndex());
        journal.flush();


        journal.compact(snapshotList.get(4));
        Assert.assertEquals(4, journal.minIndex());

        journal.compact(snapshotList.get(5));
        Assert.assertEquals(5, journal.minIndex());

        for (int partition : partitions) {
            journal.readByPartition(partition, journal.minIndex(partition));
        }

        journal.compact(snapshotList.get(12));
        Assert.assertEquals(12, journal.minIndex());

    }

    private Journal createJournal(long commitIndex) throws IOException, InterruptedException {
//        System.setProperty("PreloadBufferPool.PrintMetricIntervalMs", "500");
        Properties properties = new Properties();
        return createJournal(commitIndex, properties);
    }

    private Journal createJournal() throws IOException, InterruptedException {
        return createJournal(0L);
    }

    private Journal createJournal(Properties properties) throws IOException, InterruptedException {
        return createJournal(0L, properties);
    }

    private Journal createJournal(long commitIndex, Properties properties) throws IOException, InterruptedException {
        PersistenceFactory persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        BufferPool bufferPool = ServiceSupport.load(BufferPool.class);
        Journal journal = new Journal(
                persistenceFactory,
                bufferPool, journalEntryParser);
        journal.recover(path, commitIndex, new JournalSnapshotImpl(partitions), properties);
        return journal;
    }

    @After
    public void after() throws IOException {
        journal.close();
        TestPathUtils.destroyBaseDir();
    }

    private static class JournalSnapshotImpl implements JournalSnapshot {
        private final long minIndex;
        private final long minOffset;
        private final Map<Integer /* partition */ , Long /* min index of the partition */> partitionMinIndices;

        public JournalSnapshotImpl(Set<Integer> partitions) {
            minOffset = 0L;
            minIndex = 0L;
            partitionMinIndices = new HashMap<>(partitions.size());
            for (Integer partition : partitions) {
                partitionMinIndices.put(partition, 0L);
            }
        }

        public JournalSnapshotImpl(Journal journal) {
            this.minIndex = journal.maxIndex();
            this.minOffset = journal.maxOffset();
            this.partitionMinIndices = new HashMap<>();
            for (Integer partition : journal.getPartitions()) {
                partitionMinIndices.put(partition, journal.maxIndex(partition));
            }
        }

        @Override
        public long minIndex() {
            return minIndex;
        }

        @Override
        public long minOffset() {
            return minOffset;
        }

        @Override
        public Map<Integer, Long> partitionMinIndices() {
            return partitionMinIndices;
        }
    }
}
