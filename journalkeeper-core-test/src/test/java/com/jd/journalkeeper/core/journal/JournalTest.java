package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.test.ByteUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class JournalTest {
    private Path path = null;
    private Journal journal = null;
    @Before
    public void before() throws IOException {
       path = TestPathUtils.prepareBaseDir();

       journal = createJournal();
    }

    @Test
    public void writeReadEntryTest() {
        int maxLength = 1024;
        int size = 1024;
        int term = 8;
        List<byte []> entries = ByteUtils.createRandomSizeByteList(maxLength, size);
        List<StorageEntry> storageEntries =
                entries.stream()
                        .map(entry -> new StorageEntry(entry, term))
                        .collect(Collectors.toList());
        long maxIndex = 0L;
        for(StorageEntry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(size, maxIndex);
        Assert.assertEquals(size, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());
        long index = journal.minIndex();

        while (index < journal.maxIndex()) {
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
            index++;
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
                        .map(entry -> new StorageEntry(entry, term))
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

    private byte[] serialize(StorageEntry storageEntry) {
        byte[] serialized = new byte[storageEntry.getLength()];
        StorageEntryParser.serialize(ByteBuffer.wrap(serialized), storageEntry);
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
        List<StorageEntry> storageEntries =
                entries.stream()
                        .map(entry -> new StorageEntry(entry, 0))
                        .collect(Collectors.toList());
        for (int i = 0; i < terms.length; i++) {
            storageEntries.get(i).setTerm(terms[i]);
        }

        long maxIndex = 0L;
        for(StorageEntry storageEntry: storageEntries) {
            maxIndex = journal.append(storageEntry);
        }
        Assert.assertEquals(terms.length, maxIndex);
        Assert.assertEquals(terms.length, journal.maxIndex());
        Assert.assertEquals(0, journal.minIndex());

        int [] appendTerms = new int [] {8, 8, 9, 10, 11, 11};
        List<byte []> appendEntries = ByteUtils.createRandomSizeByteList(maxLength, appendTerms.length);
        List<StorageEntry> appendStorageEntries =
                appendEntries.stream()
                        .map(entry -> new StorageEntry(entry, 0))
                        .collect(Collectors.toList());
        for (int i = 0; i < appendTerms.length; i++) {
            appendStorageEntries.get(i).setTerm(appendTerms[i]);
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
                Assert.assertArrayEquals(storageEntries.get(i).getEntry(), journal.read(i));
            } else {
                Assert.assertArrayEquals(appendStorageEntries.get(i - startIndex).getEntry(), journal.read(i));
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
                        .map(entry -> new StorageEntry(entry, term))
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
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
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
        properties.setProperty("persistence.journal.file_data_size", String.valueOf((entrySize + StorageEntryParser.getHeaderLength()) * entriesPerFile));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(Long.BYTES * entriesPerFile));
        journal = createJournal(properties);



        List<byte []> entries = ByteUtils.createFixedSizeByteList(entrySize, size);
        List<byte []> storageEntries =
                entries.stream()
                        .map(entry -> new StorageEntry(entry, term))
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
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
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
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
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
            Assert.assertArrayEquals(entries.get((int)index), journal.read(index));
            index++;
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
                persistenceFactory.createJournalPersistenceInstance(),
                persistenceFactory.createJournalPersistenceInstance(),
                bufferPool);
        journal.recover(path,properties);
        return journal;
    }

    @After
    public void after() throws IOException {
        journal.close();
        TestPathUtils.destroyBaseDir();

    }
}
