/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.journal;

import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.entry.Entry;
import io.journalkeeper.core.entry.EntryHeader;
import io.journalkeeper.core.entry.JournalEntryParser;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.utils.ThreadSafeFormat;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import io.journalkeeper.utils.test.ByteUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import org.apache.commons.io.FileUtils;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
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
    @Before
    public void before() throws IOException {
       path = TestPathUtils.prepareBaseDir();

       journal = createJournal();
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
        List<byte []> entries = ByteUtils.createFixedSizeByteList(entrySize, batchCount);
        // 构建测试的entry
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, partition))
                        .collect(Collectors.toList());

        // 启动刷盘线程
        AsyncLoopThread flushJournalThread = ThreadBuilder.builder()
                .name("FlushJournalThread")
                .doWork(journal::flush)
                .sleepTime(50L, 50L)
                .onException(e -> logger.warn("FlushJournalThread Exception: ", e))
                .daemon(true)
                .build();

        flushJournalThread.start();
        try {
            long t0 = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                for (Entry storageEntry : storageEntries) {
                    journal.append(storageEntry);
                }
            }
            long t1 = System.nanoTime();

            long takeMs = (t1 - t0) / 1000000;
            long totalSize = (long) loopCount * batchCount * entrySize;

            logger.info("Write {} take {} ms, {}/s.",
                    Format.formatSize(totalSize),
                    takeMs,
                    Format.formatSize(totalSize * 1000 / takeMs));
            while (journal.isDirty()) {
                Thread.yield();
            }
            long t2 = System.nanoTime();
            takeMs = (t2 - t0) / 1000000;

            logger.info("Flush {} take {} ms, {}/s.",
                    Format.formatSize(totalSize),
                    takeMs,
                    Format.formatSize(totalSize * 1000 / takeMs));
        } finally {
            flushJournalThread.stop();
        }
    }
    @Test
    public void writeReadEntryTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        int partition = 6;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, partition))
                        .collect(Collectors.toList());
        long maxIndex = 0L;
        for(Entry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index).getEntry());
            index++;
        }

        index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Entry readStorageEntry = journal.read(index);
            Entry writeStorageEntry = storageEntries.get((int) index);
            Assert.assertEquals(writeStorageEntry.getHeader().getPayloadLength(), readStorageEntry.getHeader().getPayloadLength());
            Assert.assertEquals(((EntryHeader) writeStorageEntry.getHeader()).getTerm(), ((EntryHeader) readStorageEntry.getHeader()).getTerm());
            Assert.assertEquals(writeStorageEntry.getHeader().getPartition(), readStorageEntry.getHeader().getPartition());
            Assert.assertArrayEquals(writeStorageEntry.getEntry(), readStorageEntry.getEntry());
            index++;
        }

    }


    @Test
    public void readByPartitionTest() throws IOException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, (int) 0))
                        .collect(Collectors.toList());

        Map<Integer, List<Entry>> partitionEntries =
                partitions.stream().collect(Collectors.toMap(p -> p, p -> new ArrayList<>()));
        List<Integer> partitionList = new ArrayList<>(partitions);
        for (int i = 0; i < storageEntries.size(); i++) {
            int partition = partitionList.get(i % partitions.size());
            Entry entry = storageEntries.get(i);
            entry.getHeader().setPartition(partition);
            partitionEntries.get(partition).add(entry);
        }


        for(Entry storageEntry: storageEntries) {
            journal.append(storageEntry);
        }
        journal.flush();
        journal.close();

        journal = createJournal();
        partitionEntries.forEach((partition, pEntries) -> {
            Assert.assertEquals(pEntries.size(), journal.maxIndex(partition));
            Assert.assertEquals(0, journal.minIndex(partition));
            for (int i = 0; i < pEntries.size(); i++) {
                Entry pEntry = pEntries.get(i);
                RaftEntry batchEntries = journal.readByPartition(partition, i);
                Assert.assertEquals(pEntry.getHeader().getBatchSize(), batchEntries.getHeader().getBatchSize());
                Assert.assertEquals(0, batchEntries.getHeader().getOffset());
                Assert.assertArrayEquals(pEntry.getEntry(), batchEntries.getEntry());
            }
        });
    }


    @Test
    public void batchEntriesTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        int batchSize  = 23;
        int partition = 0;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, partition))
                        .peek(entry -> entry.getHeader().setBatchSize(batchSize))
                        .collect(Collectors.toList());


        for(Entry storageEntry: storageEntries) {
            journal.append(storageEntry);
        }

        Assert.assertEquals(size * batchSize , journal.maxIndex(partition));

        for (int i = 0; i < journal.maxIndex(); i++) {
            RaftEntry batchEntries = journal.readByPartition(partition, i);
            Assert.assertEquals(i % batchSize, batchEntries.getHeader().getOffset());
            Assert.assertArrayEquals(storageEntries.get(i / batchSize).getEntry(), batchEntries.getEntry());
            Assert.assertEquals(batchSize, batchEntries.getHeader().getBatchSize());
        }
    }



    @Test
    public void writeReadRawTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<byte []> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, (int) 0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendRaw(storageEntries);
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        List<byte []> readStorageEntries = journal.readRaw(index, (int )(maxIndex - index));
        Assert.assertEquals(storageEntries.size(), readStorageEntries.size());
        IntStream.range(0, readStorageEntries.size())
                .forEach(i -> Assert.assertArrayEquals(storageEntries.get(i), readStorageEntries.get(i)));

    }

    private byte[] serialize(Entry storageEntry) {
        byte[] serialized = new byte[storageEntry.getHeader().getPayloadLength() + JournalEntryParser.getHeaderLength()];
        JournalEntryParser.serialize(ByteBuffer.wrap(serialized), storageEntry);
        return serialized;
    }

    @Test
    public void compareOrAppendTest() {
        //已有的日志Terms：8, 8, 8, 8, 9, 9 , 9 , 10, 10, 10
        //新增的日志Terms：      8, 8, 9, 10, 11, 11
        //结果：          8, 8, 8, 8, 9, 10, 11, 11
        int maxLength = 128;
        int [] terms = new int [] {8, 8, 8, 8, 9, 9, 9, 10, 10, 10};
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, terms.length);
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, 0, (int) 0))
                        .collect(Collectors.toList());
        for (int i = 0; i < terms.length; i++) {
            ((EntryHeader) storageEntries.get(i).getHeader()).setTerm(terms[i]);
        }

        long maxIndex = 0L;
        for(Entry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(terms.length, maxIndex);
        Assert.assertEquals(terms.length, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        int [] appendTerms = new int [] {8, 8, 9, 10, 11, 11};
        List<byte []> appendEntries = ByteUtils.createRandomSizeByteList(maxLength, appendTerms.length);
        List<Entry> appendStorageEntries =
                appendEntries.stream()
                        .map(entry ->new Entry(entry, 0, (int) 0))
                        .collect(Collectors.toList());
        for (int i = 0; i < appendTerms.length; i++) {
            ((EntryHeader) appendStorageEntries.get(i).getHeader()).setTerm(appendTerms[i]);
        }

        List<byte []> appendRawStorageEntries = appendStorageEntries.stream()
                .map(this::serialize).collect(Collectors.toList());
        int startIndex = 2;
        journal.compareOrAppendRaw(appendRawStorageEntries, startIndex);

        Assert.assertEquals(8, journal.maxIndex());
        int [] expectedTerms = new int [] {8, 8, 8, 8, 9, 10, 11, 11};
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(expectedTerms[i], journal.getTerm(i));
            if(i < 5) {
                Assert.assertArrayEquals(storageEntries.get(i).getEntry(), journal.read(i).getEntry());
            } else {
                Assert.assertArrayEquals(appendStorageEntries.get(i - startIndex).getEntry(), journal.read(i).getEntry());
            }
        }
    }


    @Test
    public void flushRecoverTest() throws IOException {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<byte []> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, (int) 0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendRaw(storageEntries);
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
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index).getEntry());
            index++;
        }

    }


    @Test
    public void recoverTest() throws IOException {


        int entrySize = 128;
        int size = 15;
        int entriesPerFile = 5;
        int term = 8;

        journal.close();
        Properties properties = new Properties();
        properties.setProperty("persistence.journal.file_data_size", String.valueOf((entrySize + JournalEntryParser.getHeaderLength()) * entriesPerFile));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(Long.BYTES * entriesPerFile));
        journal = createJournal(properties);



        List<byte []> entries = ByteUtils.createFixedSizeByteList(entrySize, size);
        List<byte []> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, (int) 0))
                        .map(this::serialize)
                        .collect(Collectors.toList());
        long maxIndex = journal.appendRaw(storageEntries);
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        journal.flush();
        journal.close();

        // 测试多余数据是否能自动删除

        File lastFile = findLastFile(path.resolve("journal"));
        FileUtils.write(lastFile, "Extra bytes", StandardCharsets.UTF_8, true);

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index).getEntry());
            index++;
        }
        journal.flush();
        journal.close();


        // 删掉最后一条Entry的部分字节，测试是否能自动删除多余的半条Entry和索引
        lastFile = findLastFile(path.resolve("journal"));
        try(RandomAccessFile raf = new RandomAccessFile(lastFile, "rw");
            FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - entrySize / 3);
        }
        size --;

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index).getEntry());
            index++;
        }
        journal.flush();
        journal.close();

        // 删掉索引最后的15个字节（一条完整索引+一条不完整索引），测试自动重建索引
        lastFile = findLastFile(path.resolve("index"));
        try(RandomAccessFile raf = new RandomAccessFile(lastFile, "rw");
            FileChannel fileChannel = raf.getChannel()) {
            fileChannel.truncate(fileChannel.size() - Long.BYTES * 2 - 1);
        }

        journal = createJournal(properties);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        index = journal.minIndex();
        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index).getEntry());
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
        properties.setProperty("persistence.journal.file_data_size", String.valueOf((entrySize + JournalEntryParser.getHeaderLength()) * entriesPerFile));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(Long.BYTES * entriesPerFile));
        journal = createJournal(properties);



        List<byte []> entries = ByteUtils.createFixedSizeByteList(entrySize, size);
        List<Entry> storageEntries =
                entries.stream()
                        .map(entry -> new Entry(entry, term, (int) 0))
                        .collect(Collectors.toList());
        List<Integer> partitionList = new ArrayList<>(partitions);
        for (int i = 0; i < storageEntries.size(); i++) {
            int partition = partitionList.get(i % partitions.size());
            Entry entry = storageEntries.get(i);
            entry.getHeader().setPartition(partition);
        }

        long maxIndex = 0L;
        for(Entry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        journal.flush();


        journal.compact(4);
        Assert.assertEquals(0, journal.minIndex());

        journal.compact(5);
        Assert.assertEquals(5, journal.minIndex());

        for (int partition : partitions) {
            journal.readByPartition(partition, journal.minIndex(partition));
        }

        journal.compact(12);
        Assert.assertEquals(10, journal.minIndex());

        for (int partition : partitions) {
            journal.readByPartition(partition, journal.minIndex(partition));
        }
    }

    private static File findLastFile(Path parent) {

        return Arrays.stream(Objects.requireNonNull(parent.toFile()
                .listFiles(file -> file.isFile() && file.getName().matches("\\d+"))))
                .max(Comparator.comparingLong(f -> Long.parseLong(f.getName())))
                .orElse(null);

    }

    private Journal createJournal() throws IOException {
       return createJournal(new Properties());
    }
    private Journal createJournal(Properties properties) throws IOException {
        PersistenceFactory persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        BufferPool bufferPool = ServiceSupport.load(BufferPool.class);
        Journal journal = new Journal(
                persistenceFactory,
                bufferPool);
        journal.recover(path,properties);
        journal.rePartition(partitions);
        return journal;
    }

    @After
    public void after() throws IOException {
        journal.close();
        TestPathUtils.destroyBaseDir();

    }
}
