package com.jd.journalkeeper.persistence.local.journal;


import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.utils.buffer.PreloadBufferPool;
import com.jd.journalkeeper.utils.format.Format;
import com.jd.journalkeeper.utils.test.ByteUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;


/**
 * @author liyue25
 * Date: 2018-11-28
 */
public class PositioningStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(PositioningStoreTest.class);
    private Path path = null;


    // write read flush
    // single file/ multiple files
    // flush / not flush
    // cached / file

    @Test
    public void journalReadWriteTest() throws IOException {
        try (PreloadBufferPool bufferPool = new PreloadBufferPool();
             JournalPersistence store =
                new PositioningStore(bufferPool)) {
            store.recover(path, new Properties());
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte []> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = 0L;
            for(byte [] journal: journals) {
                writePosition = store.append(journal);
            }

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            byte [] readBytes = store.read(start, length);
            byte [] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);

        }
    }
    // recover
    @Test
    public void recoverTest() throws IOException {
        try (PreloadBufferPool bufferPool = new PreloadBufferPool()) {
             JournalPersistence store =
                     new PositioningStore(bufferPool);
            store.recover(path, new Properties());
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte []> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = 0L;
            for(byte [] journal: journals) {
                writePosition = store.append(journal);
            }

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            while (store.flushed() < store.max()) {
                store.flush();
            }

            store.close();
            store =
                    new PositioningStore(bufferPool);
            store.recover(path, new Properties());

            byte [] readBytes = store.read(start, length);
            byte [] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);
            store.close();
        }

    }


    // setRight

    @Test
    public void truncateTest() throws IOException {
        try (PreloadBufferPool bufferPool = new PreloadBufferPool();
             JournalPersistence store =
                     new PositioningStore(bufferPool)) {
            store.recover(path, new Properties());
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte []> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = 0L;
            for(byte [] journal: journals) {
                writePosition = store.append(journal);
            }

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            long position = IntStream.range(0,7).mapToObj(journals::get).mapToInt(journal -> journal.length).sum();
            store.truncate(position);
            Assert.assertEquals(position,store.max());

            journals.remove(9);
            journals.remove(8);
            journals.remove(7);

            byte [] readBytes = store.read(start, length);
            byte [] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);

            while (store.flushed() < store.max()) {
                store.flush();
            }

            journals.remove(6);
            position = IntStream.range(0,6).mapToObj(journals::get).mapToInt(journal -> journal.length).sum();
            store.truncate(position);

            readBytes = store.read(start, length);
            writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);

            position += 1;
            store.truncate(position);
            Assert.assertEquals(position,store.max());

        }
    }

    @Test
    public void writePerformanceTest() throws IOException, InterruptedException {
        // 总共写入消息的的大小
        long maxSize = 1024L * 1024 * 1024;
        // 每条消息消息体大小
        int batchSize = 1024 * 10;

        try (PreloadBufferPool bufferPool = new PreloadBufferPool();JournalPersistence store = prepareStore(bufferPool)) {
            write(store, maxSize, ByteUtils.createRandomSizeBytes(batchSize));
        }
    }

    @Test
    public void readPerformanceTest() throws IOException, InterruptedException, TimeoutException {
        // 总共写入消息的的大小
        long maxSize = 1024L * 1024 * 1024;
        // 每条消息消息体大小
        int batchSize = 1024 * 10;

        try (PreloadBufferPool bufferPool = new PreloadBufferPool();JournalPersistence store = prepareStore(bufferPool)) {

            write(store, maxSize, ByteUtils.createRandomSizeBytes(batchSize));
            read(store, batchSize, maxSize);
        }
    }

    private JournalPersistence prepareStore(PreloadBufferPool bufferPool) throws IOException {
        JournalPersistence store =
                new PositioningStore(bufferPool);
        store.recover(path, new Properties());
        return store;
    }
    private void read(JournalPersistence store, int batchSize, long maxSize) throws IOException{
        long start;
        long t;
        long spendTimeMs;
        long bps;

        long position = 0;

        start = System.currentTimeMillis();
        while (position < maxSize) {
            int readSize = maxSize - position > batchSize ? batchSize : (int)(maxSize - position);
            position += store.read(position, readSize).length;
        }

        t = System.currentTimeMillis();
        spendTimeMs = t - start;
        bps = maxSize * 1000 / spendTimeMs;

        logger.info("Read performance: {}/S.", Format.formatTraffic(bps));
        Assert.assertEquals(maxSize, position);
    }

    private void write(JournalPersistence store, long maxSize, byte [] journal) throws IOException {
        long currentMax = 0;
        LoopThread flushThread = LoopThread.builder()
                .doWork(store::flush)
                .sleepTime(0,0)
                .onException(e -> logger.warn("Flush Exception: ", e))
                .build();
        try {
            flushThread.start();
            long startPosition = store.max();
            long start = System.currentTimeMillis();
            while (currentMax < startPosition + maxSize) {
                currentMax = store.append(journal);
            }
            long t = System.currentTimeMillis();
            long spendTimeMs = t - start;
            long bps = (currentMax - startPosition) * 1000 / spendTimeMs;

            logger.info("Final write size: {}, write performance: {}/S.", currentMax - startPosition, Format.formatTraffic(bps));

            while (store.flushed() < store.max()) {
                Thread.yield();
            }
            t = System.currentTimeMillis();
            spendTimeMs = t - start;
            bps = (currentMax - startPosition) * 1000 / spendTimeMs;

            logger.info("Flush performance: {}/S.", Format.formatTraffic(bps));
        } finally {
            flushThread.stop();
        }
    }

    @Test
    public void shrinkTest() throws IOException, InterruptedException {
        // 总共写入消息的的大小
        long maxSize = 1024L * 1024 * 1024;
        // 每条消息消息体大小
        int batchSize = 1028;

        long shrinkPosition = 300 * 1024 * 1024;

        try (PreloadBufferPool bufferPool = new PreloadBufferPool();JournalPersistence store = prepareStore(bufferPool)) {
            write(store, maxSize, ByteUtils.createRandomSizeBytes(batchSize));

            store.shrink(shrinkPosition);
            Assert.assertTrue(store.min() <= shrinkPosition);
            Assert.assertTrue(shrinkPosition - store.min() < PositioningStore.Config.DEFAULT_FILE_DATA_SIZE);
        }


    }


    @Before
    public void before() throws Exception {
        prepareBaseDir();

    }

    @After
    public void after() {
        destroyBaseDir();

    }

    private void destroyBaseDir() {
        TestPathUtils.destroyBaseDir();

    }

    private void prepareBaseDir() throws IOException {

        path = TestPathUtils.prepareBaseDir();
        logger.info("Base directory: {}.", path);
    }

}
