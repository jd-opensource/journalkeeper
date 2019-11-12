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

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.entry.JournalEntryParseSupport;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.net.NetworkingUtils;
import io.journalkeeper.utils.test.ByteUtils;
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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.journalkeeper.core.api.RaftJournal.DEFAULT_PARTITION;

/**
 * @author LiYue
 * Date: 2019-04-25
 */
public class JournalStoreTest {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreTest.class);
    private Path base = null;
//    AbstractServer<List<byte[]>,JournalStoreQuery, List<byte[]>>


    @Before
    public void before() throws IOException {
        base = TestPathUtils.prepareBaseDir();
    }
    @After
    public void after() {
        TestPathUtils.destroyBaseDir(base.toFile());
    }


    @Test
    public void writeReadSyncOneNode() throws Exception{
        writeReadTest(1, new int [] {2}, 1024, 1,  32 , false);
    }

    @Test
    public void writeReadSyncTripleNodes() throws Exception{
        writeReadTest(3, new int [] {2, 3, 4, 5, 6}, 1024, 32, 32 , false);
    }

    @Test
    public void writeReadAsyncOneNode() throws Exception{
        writeReadTest(1, new int [] {2}, 1024, 1,  32 , true);
    }

    @Test
    public void writeReadasyncTripleNodes() throws Exception{
        writeReadTest(3, new int [] {2, 3, 4, 5, 6}, 1024, 32, 32 , true);
    }

    @Test
    public void maxPositionTest() throws IOException, ExecutionException, InterruptedException {
        List<JournalStoreServer> servers = createServers(1, base);
        JournalStoreClient client = servers.get(0).createClient();
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        Assert.assertEquals(0L, (long) maxIndices.get(0));
    }

    @Test
    public void scalePartitionTest() throws IOException, ExecutionException, InterruptedException {
        List<JournalStoreServer> servers = createServers(1, base);
        JournalStoreClient client = servers.get(0).createClient();
        Set<Integer> readPartitions = Arrays.stream(client.listPartitions().get()).boxed().collect(Collectors.toSet());
        Assert.assertEquals(Collections.singleton(DEFAULT_PARTITION), readPartitions);
        stopServers(servers);
        after();
        before();

        Set<Integer> partitions = Stream.of(2, 3, 4, 99, 8).collect(Collectors.toSet());
        servers = createServers(1, base, partitions);

        client = servers.get(0).createClient();
        readPartitions = Arrays.stream(client.listPartitions().get()).boxed().collect(Collectors.toSet());
        Assert.assertEquals(partitions, readPartitions);
        int [] intPartitions = new int [] {2, 3, 7, 8};
        AdminClient adminClient = servers.get(0).getAdminClient();
        adminClient.scalePartitions(intPartitions).get();
        int [] readIntPartitions = client.listPartitions().get();
        Arrays.sort(readIntPartitions);
        Assert.assertArrayEquals(intPartitions, readIntPartitions);
        stopServers(servers);

        after();
        before();

        partitions = Stream.of(2, 3, 4, 99, 8).collect(Collectors.toSet());
        URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
        Path workingDir = base.resolve("server-recover");
        Properties properties = new Properties();
        properties.setProperty("working_dir", workingDir.toString());
        JournalStoreServer journalStoreServer  = new JournalStoreServer(properties);
        journalStoreServer.init(uri, Collections.singletonList(uri), partitions);
        journalStoreServer.recover();
        journalStoreServer.start();

        client = journalStoreServer.createLocalClient();
        readPartitions = Arrays.stream(client.listPartitions().get()).boxed().collect(Collectors.toSet());
        Assert.assertEquals(partitions, readPartitions);

        journalStoreServer.stop();

        journalStoreServer  = new JournalStoreServer(properties);
        journalStoreServer.recover();
        journalStoreServer.start();

        client = journalStoreServer.createLocalClient();
        readPartitions = Arrays.stream(client.listPartitions().get()).boxed().collect(Collectors.toSet());
        Assert.assertEquals(partitions, readPartitions);

        journalStoreServer.stop();

    }
    @Test
    public void queryIndexTest() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        JournalEntryParser journalEntryParser =new DefaultJournalEntryParser();
        JournalStoreServer server = createServers(1, base).get(0);

        JournalStoreClient client = server.createLocalClient();
        client.waitForClusterReady();

        long [] timestamps = new long[] {10, 11, 12 , 100, 100, 101, 105, 110, 2000};
        long [] indices = new long[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            long timestamp = timestamps[i];
            byte[] payload = new byte[128];
            JournalEntry entry = journalEntryParser.createJournalEntry(payload);
            byte[] rawEntry = entry.getSerializedBytes();
            JournalEntryParseSupport.setLong(ByteBuffer.wrap(rawEntry),
                    JournalEntryParseSupport.TIMESTAMP, timestamp);

            long index = client.append(0, 1, rawEntry, true, ResponseConfig.REPLICATION).get();
            indices[i] = index;
        }

        Assert.assertEquals(indices[0], client.queryIndex(0, 8L).get().longValue());
        Assert.assertEquals(indices[0], client.queryIndex(0, 10L).get().longValue());
        Assert.assertEquals(indices[2], client.queryIndex(0, 80L).get().longValue());
        Assert.assertEquals(indices[3], client.queryIndex(0, 100L).get().longValue());
        Assert.assertEquals(indices[8], client.queryIndex(0, 2000L).get().longValue());
        Assert.assertEquals(indices[8], client.queryIndex(0, 9000L).get().longValue());

        server.stop();


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
            client.waitForClusterReady();
            AdminClient adminClient = servers.get(0).getAdminClient();
            adminClient.scalePartitions(partitions).get();

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
                    "Write takes: {}ms, {}ps, tps: {}.",
                     (t1 - t0) / 1000000,
                    Format.formatSize( 1000000000L * partitions.length * entrySize * batchCount  / (t1 - t0)),
                    1000000000L * partitions.length * batchCount  / (t1 - t0));


            // read

            t0 = System.nanoTime();
            for (int partition : partitions) {
                for (int i = 0; i < batchCount; i++) {
                    List<JournalEntry> raftEntries = client.get(partition, i * batchSize, batchSize).get();
                    Assert.assertEquals(raftEntries.size(), 1);
                    JournalEntry entry = raftEntries.get(0);
                    Assert.assertEquals(partition, entry.getPartition());
                    Assert.assertEquals(batchSize, entry.getBatchSize());
                    Assert.assertEquals(0, entry.getOffset());
                    Assert.assertArrayEquals(rawEntries, entry.getSerializedBytes());
                }
            }
            t1 = System.nanoTime();
            logger.info("Read finished. " +
                            "Takes: {}ms {}ps, tps: {}.",
                    (t1 - t0) / 1000000,
                    Format.formatSize( 1000000000L * partitions.length * entrySize * batchCount  / (t1 - t0)),
                    1000000000L * partitions.length * batchCount  / (t1 - t0));
        } finally {
            stopServers(servers);

        }

    }
    @Test
    public void allResponseTest() throws Exception {
        int nodes = 5;
        int entrySize = 1024;
        int count = 512;
        List<JournalStoreServer> servers = createServers(nodes, base);
        try {
            JournalStoreClient client = servers.get(0).createClient();
            client.waitForClusterReady();

            byte[] rawEntries = ByteUtils.createFixedSizeBytes(entrySize);
            CompletableFuture[] futures = new CompletableFuture [count];
            for (int i = 0; i < count; i++) {
                futures[i] = client.append(0, 1, rawEntries, ResponseConfig.ALL);
            }

            CompletableFuture.allOf(futures).get();

            for (int i = 0; i < count; i++) {
                List<JournalEntry> raftEntries = client.get(0, i, 1).get();
                Assert.assertEquals(raftEntries.size(), 1);
                JournalEntry entry = raftEntries.get(0);
                Assert.assertEquals(0, entry.getPartition());
                Assert.assertEquals(1, entry.getBatchSize());
                Assert.assertEquals(0, entry.getOffset());
                Assert.assertArrayEquals(rawEntries, entry.getPayload().getBytes());
            }

        } finally {
            stopServers(servers);

        }

    }

    @Test
    public void transactionCommitTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        UUID transactionId = client.createTransaction().get();
        Assert.assertNotNull(transactionId);

        // Send some transactional messages
        CompletableFuture [] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionId, rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<UUID> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionId));

        // Commit the transaction
        client.completeTransaction(transactionId, true).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionId));

        // Verify messages
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            int index = i / partitions.size();
            List<JournalEntry> journalEntries = client.get(partition, index, 1).get();
            Assert.assertEquals(1, journalEntries.size());
            JournalEntry journalEntry = journalEntries.get(0);
            byte [] readEntry = journalEntry.getPayload().getBytes();

            Assert.assertArrayEquals(rawEntries.get(i), readEntry);

        }

        stopServers(servers);

    }

    @Test
    public void transactionAbortTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        UUID transactionId = client.createTransaction().get();
        Assert.assertNotNull(transactionId);

        // Send some transactional messages
        CompletableFuture [] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionId, rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<UUID> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionId));

        // Commit the transaction
        client.completeTransaction(transactionId, false).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionId));

        // Verify messages
        maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        stopServers(servers);

    }


    @Test
    public void transactionFailSafeTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 10;

        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        List<JournalStoreServer> servers = createServers(nodes, base, partitions);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        UUID transactionId = client.createTransaction().get();
        Assert.assertNotNull(transactionId);

        // Send some transactional messages
        CompletableFuture [] futures = new CompletableFuture[rawEntries.size() / 2];
        int i = 0;
        for (; i < rawEntries.size() / 2; i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionId, rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // Stop current leader and wait until new leader is ready.
        AdminClient adminClient = servers.get(0).getAdminClient();
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        JournalStoreServer leaderServer = servers.stream().filter(server -> leaderUri.equals(server.serverUri())).findAny().orElse(null);
        Assert.assertNotNull(leaderServer);

        leaderServer.stop();
        client.waitForClusterReady();

        // Send some transactional messages
        futures = new CompletableFuture[rawEntries.size() - rawEntries.size() / 2];
        for (; i < rawEntries.size() ; i++) {
            int partition = i % partitions.size();
            futures[i - rawEntries.size() / 2] = client.append(transactionId, rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();


        // Verify no message is readable until the transaction was committed.
        Map<Integer, Long> maxIndices = client.maxIndices().get();
        for (Long maxIndex : maxIndices.values()) {
            Assert.assertEquals(0L, (long) maxIndex);
        }

        Collection<UUID> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertTrue(openingTransactions.contains(transactionId));

        // Commit the transaction
        client.completeTransaction(transactionId, true).get();

        openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionId));

        // Verify messages
        for (i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            int index = i / partitions.size();
            List<JournalEntry> journalEntries = client.get(partition, index, 1).get();
            Assert.assertEquals(1, journalEntries.size());
            JournalEntry journalEntry = journalEntries.get(0);
            byte [] readEntry = journalEntry.getPayload().getBytes();

            Assert.assertArrayEquals(rawEntries.get(i), readEntry);

        }

        stopServers(servers);

    }

    @Test
    public void transactionTimeoutTest() throws Exception {
        final int nodes = 3;
        final int entrySize = 1024;
        final int entryCount = 3;
        final long transactionTimeoutMs = 5000L;
        final Set<Integer> partitions = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(0, 1, 2, 3, 4))
        );
        List<byte[]> rawEntries = ByteUtils.createFixedSizeByteList(entrySize, entryCount);
        Properties properties = new Properties();
        properties.put("transaction_timeout_ms", String.valueOf(transactionTimeoutMs));
        List<JournalStoreServer> servers = createServers(nodes, base, partitions, properties);
        JournalStoreClient client = new JournalStoreClient(servers.stream().map(JournalStoreServer::serverUri).collect(Collectors.toList()), new Properties());
        client.waitForClusterReady();

        // Create transaction
        UUID transactionId = client.createTransaction().get();
        Assert.assertNotNull(transactionId);

        // Send some transactional messages
        CompletableFuture [] futures = new CompletableFuture[rawEntries.size()];
        for (int i = 0; i < rawEntries.size(); i++) {
            int partition = i % partitions.size();
            futures[i] = client.append(transactionId, rawEntries.get(i), partition, 1);
        }
        CompletableFuture.allOf(futures).get();

        // wait for transaction timeout
        logger.info("Wait {} ms for transaction timeout...", transactionTimeoutMs * 2);
        Thread.sleep(transactionTimeoutMs * 2);

        Collection<UUID> openingTransactions = client.getOpeningTransactions().get();
        Assert.assertFalse(openingTransactions.contains(transactionId));

        stopServers(servers);

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
                Format.formatSize( 1000000000L * partitions.length * rawEntries.length * batchCount  / (t1 - t0)));

        while (!latch.await(1, TimeUnit.SECONDS)) {
            Thread.yield();
        }
        logger.warn("Async write exceptions: {}.", exceptions.size());
        exceptions.stream().limit(10).forEach(e -> logger.warn("Exception: ", e));
        Assert.assertEquals(0, exceptions.size());

    }

    private void asyncAppend(JournalStoreClient client, byte[] rawEntries, int partition, int batchSize, CountDownLatch latch, ExecutorService executorService, List<Throwable> exceptions) {
        client.append(partition, batchSize, rawEntries, ResponseConfig.REPLICATION)
                .whenComplete((v, e) -> {

                    if(e instanceof CompletionException && e.getCause() instanceof ServerBusyException) {
//                        logger.info("AbstractServer busy!");
                        Thread.yield();
                        asyncAppend(client, rawEntries, partition, batchSize, latch, executorService, exceptions);

                    } else {
                        if(null != e) {
                            logger.warn("Exception: ", e);
                            exceptions.add(e);
                        }
                        latch.countDown();
                    }
                });
    }

    private void syncWrite(int[] partitions, int batchSize, int batchCount, JournalStoreClient client, byte[] rawEntries) throws InterruptedException, ExecutionException {
        // write
        for (int partition : partitions) {
            for (int i = 0; i < batchCount; i++) {
                client.append(partition, batchSize, rawEntries,ResponseConfig.REPLICATION).get();
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
        return createServers(nodes, path, Collections.singleton(0));
    }

    private List<JournalStoreServer> createServers(int nodes, Path path, Set<Integer> partitions) throws IOException {
        return createServers(nodes, path, partitions, null);
    }

    private List<JournalStoreServer> createServers(int nodes, Path path, Set<Integer> partitions, Properties props) throws IOException {
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
            properties.setProperty("rpc_timeout_ms", "600000");
            properties.setProperty("cache_requests", String.valueOf(1024L * 1024));

            if(null != props) {
                properties.putAll(props);
            }
//            properties.setProperty("print_metric_interval_sec", String.valueOf(1));
//            properties.setProperty("enable_metric", String.valueOf(true));
//            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
//            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, partitions);
    }

    private List<JournalStoreServer> createServers(List<URI> serverURIs, List<Properties> propertiesList, Set<Integer> partitions) throws IOException {
        List<JournalStoreServer> journalStoreServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            JournalStoreServer journalStoreServer  = new JournalStoreServer(propertiesList.get(i));
            journalStoreServers.add(journalStoreServer);
            journalStoreServer.init(serverURIs.get(i), serverURIs, partitions);
            journalStoreServer.recover();
            journalStoreServer.start();
        }
        return journalStoreServers;
    }
}
