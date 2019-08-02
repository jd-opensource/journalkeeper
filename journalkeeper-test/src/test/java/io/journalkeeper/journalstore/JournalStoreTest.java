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
package io.journalkeeper.journalstore;

import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.net.NetworkingUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author LiYue
 * Date: 2019-04-25
 */
public class JournalStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreTest.class);
    private Path base = null;
//    Server<List<byte[]>,JournalStoreQuery, List<byte[]>>


    @Before
    public void before() throws IOException {
        base = TestPathUtils.prepareBaseDir();
    }
    @After
    public void after() {
        TestPathUtils.destroyBaseDir(base.toFile());
    }


    @Test
    public void writeReadOneNode() throws Exception{
        writeReadTest(1, new int [] {2, 3, 4, 5, 6}, 1024, 1024, 1024 * 100, true);
    }

    @Test
    public void writeReadTripleNodes() throws Exception{
        writeReadTest(3, new int [] {2, 3, 4, 5, 6}, 1024, 1024, 1024 , false);
    }

    /**
     * 读写测试
     * @param nodes 节点数量
     * @param partitions 分区数量
     * @param entrySize 数据大小
     * @param batchSize 每批数据条数
     * @param batchCount 批数
     */
    private void writeReadTest(int nodes, int [] partitions, int entrySize, int batchSize, int batchCount, boolean async) throws Exception {
        List<JournalStoreServer> servers = createServers(nodes, base);
        try {
            JournalStoreClient client = servers.get(0).createClient();
            client.waitForLeader(10000);

            client.scalePartitions(partitions).get();

            // Wait for all node to finish scale partitions.
            Thread.sleep(1000L);

            long t0 = System.nanoTime();
            byte[] rawEntries = new byte[entrySize];
            for (int i = 0; i < rawEntries.length; i++) {
                rawEntries[i] = (byte) (i % Byte.MAX_VALUE);
            }
            if(async) {
                asyncWrite(partitions, batchSize, batchCount, client, rawEntries);
            } else {
                syncWrite(partitions, batchSize, batchCount, client, rawEntries);
            }
            long t1 = System.nanoTime();
            logger.info("Replication finished. " +
                    "Write takes: {}ms {}ps",
                     (t1 - t0) / 1000000,
                    Format.formatSize( 1000000000L * entrySize * batchCount  / (t1 - t0)));


            // read

            t0 = System.nanoTime();
            for (int partition : partitions) {
                for (int i = 0; i < batchCount; i++) {
                    List<RaftEntry> raftEntries = client.get(partition, i * batchSize, batchSize).get();
                    Assert.assertEquals(raftEntries.size(), 1);
                    RaftEntry entry = raftEntries.get(0);
                    Assert.assertEquals(partition, entry.getHeader().getPartition());
                    Assert.assertEquals(batchSize, entry.getHeader().getBatchSize());
                    Assert.assertEquals(0, entry.getHeader().getOffset());
                    Assert.assertArrayEquals(rawEntries, entry.getEntry());
                }
            }
            t1 = System.nanoTime();
            logger.info("Read finished. " +
                            "Takes: {}ms {}ps.",
                    (t1 - t0) / 1000000,
                    Format.formatSize( 1000000000L * entrySize * batchCount  / (t1 - t0)));

        } finally {
            stopServers(servers);

        }

    }

    private void asyncWrite(int[] partitions, int batchSize, int batchCount, JournalStoreClient client, byte[] rawEntries) throws InterruptedException {
        ExecutorService executors = Executors.newFixedThreadPool(10, new NamedThreadFactory("ClientRetryThreads"));
        CountDownLatch latch = new CountDownLatch(partitions.length * batchCount);
        final List<Throwable> exceptions = Collections.synchronizedList(new LinkedList<>());
        long t0 = System.nanoTime();
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {
                asyncAppend(client, rawEntries, partition, batchSize, latch, executors, exceptions);
            }
        }
        long t1 = System.nanoTime();
        logger.info("Async write finished. " +
                        "Takes: {}ms {}ps.",
                (t1 - t0) / 1000000,
                Format.formatSize( 1000000000L * rawEntries.length * batchCount  / (t1 - t0)));

        while (!latch.await(1, TimeUnit.SECONDS)) {
            Thread.yield();
        }
        logger.warn("Async write exceptions: {}.", exceptions.size());
        exceptions.stream().limit(10).forEach(e -> logger.warn("Exception: ", e));
        Assert.assertEquals(0, exceptions.size());

    }

    private void asyncAppend(JournalStoreClient client, byte[] rawEntries, int partition, int batchSize, CountDownLatch latch, ExecutorService executorService, List<Throwable> exceptions) {
        client.append(partition, batchSize, rawEntries, ResponseConfig.RECEIVE)
                .whenCompleteAsync((v, e) -> {

                    if(e instanceof CompletionException && e.getCause() instanceof ServerBusyException) {
//                        logger.info("Server busy!");
                        Thread.yield();
                        asyncAppend(client, rawEntries, partition, batchSize, latch, executorService, exceptions);

                    } else {
                        if(null != e) {
                            logger.warn("Exception: ", e);
                            exceptions.add(e);
                        }
                        latch.countDown();
                    }
                }, executorService);
    }

    private void syncWrite(int[] partitions, int batchSize, int batchCount, JournalStoreClient client, byte[] rawEntries) throws InterruptedException, ExecutionException {
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {
                client.append(partition, batchSize, rawEntries).get();
            }
        }
    }

    private void stopServers(List<JournalStoreServer> servers) {
        for (JournalStoreServer server : servers) {
            try {
                server.stop();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception: ", server.serverUri(), t);
            }
        }
    }

    private List<JournalStoreServer> createServers(int nodes, Path path) throws IOException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("snapshot_step", "0");
            properties.setProperty("cache_requests", "102400");
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList);
    }

    private List<JournalStoreServer> createServers(List<URI> serverURIs, List<Properties> propertiesList) throws IOException {
        List<JournalStoreServer> journalStoreServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            JournalStoreServer journalStoreServer  = new JournalStoreServer(propertiesList.get(i));
            journalStoreServers.add(journalStoreServer);
            journalStoreServer.init(serverURIs.get(i), serverURIs);
            journalStoreServer.recover();
            journalStoreServer.start();
        }
        return journalStoreServers;
    }
}
