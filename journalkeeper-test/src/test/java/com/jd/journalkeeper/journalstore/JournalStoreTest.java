package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.ServerBusyException;
import com.jd.journalkeeper.utils.format.Format;
import com.jd.journalkeeper.utils.net.NetworkingUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liyue25
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
        writeReadTest(1, new int [] {2, 3, 4, 5, 6}, 1024, 1024, 1024, true);
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
        CountDownLatch latch = new CountDownLatch(partitions.length * batchCount);
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {

                asyncAppend(client, rawEntries, partition, batchSize, latch);
            }
        }


        logger.info("Write finished, waiting for responses...");

        while (!latch.await(1, TimeUnit.SECONDS)) {
            Thread.yield();
        }

    }

    private void asyncAppend(JournalStoreClient client, byte[] rawEntries, int partition, int batchSize, CountDownLatch latch) {
        client.append(partition, batchSize, rawEntries)
                .whenComplete((v, e) -> {

                    if(e instanceof CompletionException && e.getCause() instanceof ServerBusyException) {
                        Thread.yield();
                        asyncAppend(client, rawEntries, partition, batchSize, latch);

                    } else {
                        Assert.assertNull(e);
                        latch.countDown();
                    }
                });
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
