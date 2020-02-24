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
package io.journalkeeper.persistence.local.journal;


import io.journalkeeper.persistence.JournalPersistence;
import io.journalkeeper.utils.format.Format;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;


/**
 * @author LiYue
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
    public void journalReadWriteTest() throws IOException, InterruptedException {
        try (JournalPersistence store = prepareStore()) {
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte[]> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = 0L;
            for (byte[] journal : journals) {
                writePosition = store.append(journal);
            }

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            byte[] readBytes = store.read(start, length);
            byte[] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);
        }

    }


    @Test
    public void batchWriteTest() throws IOException, InterruptedException {
        try (JournalPersistence store = prepareStore()) {
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte[]> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = store.append(journals);

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            byte[] readBytes = store.read(start, length);
            byte[] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);
        }

    }

    // recover
    @Test
    public void recoverTest() throws IOException {
        JournalPersistence store =
                new PositioningStore();
        store.recover(path, new Properties());
        int size = 10;
        int maxLength = 999;
        long start = store.max();
        List<byte[]> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
        int length = journals.stream().mapToInt(journal -> journal.length).sum();

        long writePosition = 0L;
        for (byte[] journal : journals) {
            writePosition = store.append(journal);
        }

        Assert.assertEquals(length + start, writePosition);
        Assert.assertEquals(writePosition, store.max());

        while (store.flushed() < store.max()) {
            store.flush();
        }

        store.close();
        store =
                new PositioningStore();
        store.recover(path, new Properties());

        byte[] readBytes = store.read(start, length);
        byte[] writeBytes = ByteUtils.concatBytes(journals);
        Assert.assertArrayEquals(writeBytes, readBytes);
        store.close();

    }


    @Test(expected = IllegalArgumentException.class)
    public void truncateTest() throws IOException, InterruptedException {
        try (JournalPersistence store = prepareStore()) {
            int size = 10;
            int maxLength = 999;
            long start = store.max();
            List<byte[]> journals = ByteUtils.createRandomSizeByteList(maxLength, size);
            int length = journals.stream().mapToInt(journal -> journal.length).sum();

            long writePosition = 0L;
            for (byte[] journal : journals) {
                writePosition = store.append(journal);
            }

            Assert.assertEquals(length + start, writePosition);
            Assert.assertEquals(writePosition, store.max());

            long position = IntStream.range(0, 7).mapToObj(journals::get).mapToInt(journal -> journal.length).sum();
            store.truncate(position);
            Assert.assertEquals(position, store.max());

            journals.remove(9);
            journals.remove(8);
            journals.remove(7);

            byte[] readBytes = store.read(start, length);
            byte[] writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);

            while (store.flushed() < store.max()) {
                store.flush();
            }

            journals.remove(6);
            position = IntStream.range(0, 6).mapToObj(journals::get).mapToInt(journal -> journal.length).sum();
            store.truncate(position);

            readBytes = store.read(start, length);
            writeBytes = ByteUtils.concatBytes(journals);
            Assert.assertArrayEquals(writeBytes, readBytes);

            position += 1;
            store.truncate(position);

        }
    }

    @Ignore
    @Test
    public void writePerformanceTest() throws IOException, InterruptedException {
        // 总共写入消息的的大小
        long maxSize = 10L * 1024L * 1024 * 1024;
        // 每条消息消息体大小
        int logSize = 1024;

        try (JournalPersistence store = prepareStore()) {
            write(store, maxSize, ByteUtils.createFixedSizeBytes(logSize));
        }
    }

    @Ignore
    @Test
    public void readPerformanceTest() throws IOException, InterruptedException {
        // 总共写入消息的的大小
        long maxSize = 1024L * 1024 * 1024;
        // 每条消息消息体大小
        int batchSize = 1024 * 10;

        try (JournalPersistence store =
                     prepareStore()) {
            write(store, maxSize, ByteUtils.createRandomSizeBytes(batchSize));
            read(store, batchSize, maxSize);
        }
    }

    private JournalPersistence prepareStore() throws IOException, InterruptedException {
        JournalPersistence store =
                new PositioningStore();
        Properties properties = new Properties();
        properties.put("cached_file_core_count", "5");
        properties.put("cached_file_max_count", "20");
        properties.put("max_dirty_size", String.valueOf(200L * 1024 * 1024));
        store.recover(path, properties);
        Thread.sleep(1000L);
        return store;
    }

    private void read(JournalPersistence store, int batchSize, long maxSize) throws IOException {
        long start;
        long t;
        long spendTimeMs;
        long bps;

        long position = 0;

        start = System.currentTimeMillis();
        while (position < maxSize) {
            int readSize = maxSize - position > batchSize ? batchSize : (int) (maxSize - position);
            position += store.read(position, readSize).length;
        }

        t = System.currentTimeMillis();
        spendTimeMs = t - start;
        bps = maxSize * 1000 / spendTimeMs;

        logger.info("Read performance: {}/S.", Format.formatSize(bps));
        Assert.assertEquals(maxSize, position);
    }

    private void write(JournalPersistence store, long maxSize, byte[] journal) throws IOException {
        long currentMax = 0;
        AsyncLoopThread flushThread = ThreadBuilder.builder()
                .doWork(store::flush)
                .sleepTime(0, 0)
                .onException(e -> logger.warn("Flush Exception: ", e))
                .daemon(true)
                .build();
        try {
            flushThread.start();
            long startPosition = store.max();
            long start = System.currentTimeMillis();
            while (currentMax < startPosition + maxSize) {
                currentMax = store.append(journal);
            }
            long t = System.currentTimeMillis();
            long spendTimeNs = t - start;
            long bps = (currentMax - startPosition) * 1000 / spendTimeNs;

            logger.info("Final write size: {}, write performance: {}/S.",
                    Format.formatSize(currentMax - startPosition),
                    Format.formatSize(bps));

            while (store.flushed() < store.max()) {
                Thread.yield();
            }
            t = System.currentTimeMillis();
            spendTimeNs = t - start;
            bps = (currentMax - startPosition) * 1000 / spendTimeNs;

            logger.info("Flush performance: {}/S.", Format.formatSize(bps));
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

        try (JournalPersistence store = prepareStore()) {
            write(store, maxSize, ByteUtils.createRandomSizeBytes(batchSize));

            store.compact(shrinkPosition);
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
