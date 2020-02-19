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
package io.journalkeeper.core.server;

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.ScalePartitionsEntry;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricSupport;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.test.ByteUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author LiYue
 * Date: 2019-08-05
 */
public class VoterTest {
    private static final Logger logger = LoggerFactory.getLogger(VoterTest.class);
    private Path base = null;

    @Before
    public void before() throws IOException {
        base = TestPathUtils.prepareBaseDir();
//        System.setProperty("PreloadBufferPool.PrintMetricIntervalMs", "1000");
    }

    @After
    public void after() {
        TestPathUtils.destroyBaseDir(base.toFile());
    }

    @Ignore
    @Test
    public void singleNodeWritePerformanceTest() throws IOException, ExecutionException, InterruptedException {
        Server voter = createVoter();


        try {
            int count = 10 * 1024 * 1024;
            int entrySize = 1024;
            Set<Integer> partitions = Stream.of(2).collect(Collectors.toSet());

            while (voter.getServerStatus().get().getServerStatus().getVoterState() != VoterState.LEADER) {
                Thread.sleep(50L);
            }

            voter.updateClusterState(new UpdateClusterStateRequest(
                    InternalEntriesSerializeSupport
                            .serialize(new ScalePartitionsEntry(partitions)),
                    RaftJournal.INTERNAL_PARTITION, 1)).get();


            byte[] entry = ByteUtils.createFixedSizeBytes(entrySize);
            long t0 = System.nanoTime();
            for (Integer partition : partitions) {
                UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry, partition, 1);

                for (long l = 0; l < count; l++) {
                    voter.updateClusterState(request);
                }

            }

            long t1 = System.nanoTime();
            long takesMs = (t1 - t0) / 1000000;
            logger.info("Write finished. " +
                            "Write takes: {}ms, {}ps, tps: {}.",
                    takesMs,
                    Format.formatSize(1000L * partitions.size() * entrySize * count / takesMs),
                    1000L * partitions.size() * count / takesMs);

        } finally {
            voter.stop();
        }


    }

    @Ignore
    @Test
    public void multiThreadsWritePerformanceTest() throws IOException, ExecutionException, InterruptedException {
        Server voter = createVoter();


        try {
            int count = 10 * 1024 * 1024;
            int entrySize = 1024;
            int threads = 1;
            Set<Integer> partitions = Stream.of(2, 3, 4, 5, 6).collect(Collectors.toSet());

            while (voter.getServerStatus().get().getServerStatus().getVoterState() != VoterState.LEADER) {
                Thread.sleep(50L);
            }

            voter.updateClusterState(new UpdateClusterStateRequest(
                    InternalEntriesSerializeSupport
                            .serialize(new ScalePartitionsEntry(partitions)),
                    RaftJournal.INTERNAL_PARTITION, 1)).get();


            byte[] entry = ByteUtils.createFixedSizeBytes(entrySize);

            CountDownLatch threadLatch = new CountDownLatch(threads);
            AtomicInteger currentCount = new AtomicInteger(0);

            JMetricFactory factory = ServiceSupport.load(JMetricFactory.class);
            JMetric metric = factory.create("WRITE");

            Iterator<Integer> partitionsIterator = partitions.iterator();
            for (int i = 0; i < threads; i++) {
                if (!partitionsIterator.hasNext()) {
                    partitionsIterator = partitions.iterator();
                }
                int partition = partitionsIterator.next();

                Thread t = new Thread(() -> {
                    UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry, partition, 1);
                    while (currentCount.incrementAndGet() <= count) {
                        try {
                            long t0 = System.nanoTime();
                            voter.updateClusterState(request).get();
                            metric.mark(System.nanoTime() - t0, entry.length);
                        } catch (Throwable e) {
                            logger.warn("Exception: ", e);
                            break;
                        }
                    }
                    threadLatch.countDown();
                });
                t.setName("ClientThread-" + i);
                t.start();
            }

            threadLatch.await();

            logger.info(JMetricSupport.formatNs(metric.get()));

        } finally {
            voter.stop();
        }
    }

    @Test
    public void updateClusterStateResultTest() throws IOException, ExecutionException, InterruptedException {
        Server voter = createVoter();


        try {
            byte[] entry = new byte[]{1, 2, 3, 5, 8, 9};
            while (voter.getServerStatus().get().getServerStatus().getVoterState() != VoterState.LEADER) {
                Thread.sleep(50L);
            }


            logger.info("Send UpdateClusterStateRequest...");
            UpdateClusterStateResponse response;
            do {
                Thread.sleep(10);
                response = voter.updateClusterState(new UpdateClusterStateRequest(entry, RaftJournal.DEFAULT_PARTITION, 1)).get();
            } while (response.getStatusCode() == StatusCode.NOT_LEADER);
            Assert.assertTrue(response.success());
            Assert.assertArrayEquals(entry, response.getResults().get(0));

        } finally {
            voter.stop();
        }


    }


    @Test
    public void batchUpdateClusterStateTest() throws IOException, ExecutionException, InterruptedException {
        Server voter = createVoter();


        try {
            List<byte[]> bytes = ByteUtils.createRandomSizeByteList(1024, 128);
            while (voter.getServerStatus().get().getServerStatus().getVoterState() != VoterState.LEADER) {
                Thread.sleep(50L);
            }
            List<UpdateRequest> requests = bytes.stream()
                    .map(byteArray -> new UpdateRequest(byteArray, RaftJournal.DEFAULT_PARTITION, 1))
                    .collect(Collectors.toList());
            logger.info("Send UpdateClusterStateRequest...");

            UpdateClusterStateResponse response =
                    voter.updateClusterState(
                            new UpdateClusterStateRequest(
                                    requests
                            )).get();
            Assert.assertTrue(response.success());
            Assert.assertEquals(bytes.size(), response.getResults().size());
            for (int i = 0; i < bytes.size(); i++) {
                Assert.assertArrayEquals(bytes.get(i), response.getResults().get(i));
            }

        } finally {
            voter.stop();
        }
    }


    // Running this test case takes about 4 minutes.
    @Ignore
    @Test
    public void journalCompactionTest() throws Exception {
        Properties properties = new Properties();
        properties.put("snapshot_interval_sec", "30");
        properties.put("journal_retention_min", "1");
//        properties.setProperty("enable_metric", "true");
//        properties.setProperty("print_metric_interval_sec", "3");
        Server voter = createVoter(properties);
        try {
            while (voter.getServerStatus().get().getServerStatus().getVoterState() != VoterState.LEADER) {
                Thread.sleep(50L);
            }

            for (int i = 0; i < 4 * 60; i++) {
                byte[] byteArray = ByteUtils.createFixedSizeBytes(1024);
                voter.updateClusterState(new UpdateClusterStateRequest(
                        byteArray, RaftJournal.DEFAULT_PARTITION, 1
                ));
                Thread.sleep(1000L);
            }
            ServerStatus serverStatus = voter.getServerStatus().get().getServerStatus();
            logger.info("{}.", serverStatus);
            Assert.assertNotEquals(0L, serverStatus.getMinIndex());

        } finally {
            voter.stop();
        }
    }


    private Server createVoter() throws IOException {
        return createVoter(null);
    }

    private Server createVoter(Properties customProperties) throws IOException {
        StateFactory stateFactory = new NoopStateFactory();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4, new NamedThreadFactory("JournalKeeper-Scheduled-Executor"));
        ExecutorService asyncExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, new NamedThreadFactory("JournalKeeper-Async-Executor"));
        Properties properties = new Properties();
        properties.setProperty("working_dir", base.toString());
        if (null != customProperties) {
            properties.putAll(customProperties);
        }
//        properties.setProperty("enable_metric", "true");
//        properties.setProperty("print_metric_interval_sec", "3");
//        properties.setProperty("cache_requests", String.valueOf(1024L * 1024 * 5));

        Server voter =
                new Server(
                        RaftServer.Roll.VOTER,
                        stateFactory, new DefaultJournalEntryParser(),
                        scheduledExecutorService, asyncExecutorService, properties);
        URI uri = URI.create("local://test");
        voter.init(uri, Collections.singletonList(uri));
        voter.recover();
        voter.start();
        return voter;
    }

    static class EchoState implements State {
        @Override
        public StateResult execute(byte[] entry, int partition, long index, int batchSize, RaftJournal raftJournal) {
            return new StateResult(entry);
        }


        @Override
        public void recover(Path path, Properties properties) {
        }

        @Override
        public byte[] query(byte[] query, RaftJournal raftJournal) {
            return query;
        }
    }


    static class NoopStateFactory implements StateFactory {

        @Override
        public State createState() {
            return new EchoState();
        }
    }
}
