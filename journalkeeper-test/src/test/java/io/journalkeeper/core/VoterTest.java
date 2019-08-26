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
package io.journalkeeper.core;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.server.Voter;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricSupport;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    }

    @After
    public void after() {
        TestPathUtils.destroyBaseDir(base.toFile());
    }

    @Ignore
    @Test
    public void singleNodeWritePerformanceTest() throws IOException, ExecutionException, InterruptedException {
        Voter<byte[],byte[], byte[], byte[]> voter = createVoter();


        try {
            int count = 10 * 1024 * 1024;
            int entrySize = 1024;
            int[] partitions = {2};

            while(voter.voterState() != Voter.VoterState.LEADER) {
                Thread.sleep(50L);
            }

            voter.updateClusterState(new UpdateClusterStateRequest(
                    JdkSerializerFactory.createSerializer(ScalePartitionsEntry.class)
                            .serialize(new ScalePartitionsEntry(partitions)),
                    RaftJournal.RESERVED_PARTITION, 1)).get();


            byte[] entry = ByteUtils.createFixedSizeBytes(entrySize);
            long t0 = System.nanoTime();

            for (int i = 0; i < partitions.length; i++) {
                UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry, partitions[i], 1);

                for (long l = 0; l < count; l++) {
                    voter.updateClusterState(request);
                }
            }


            long t1 = System.nanoTime();
            long takesMs = (t1 - t0) / 1000000;
            logger.info("Write finished. " +
                            "Write takes: {}ms, {}ps, tps: {}.",
                    takesMs,
                    Format.formatSize( 1000L * partitions.length * entrySize * count  / takesMs),
                    1000L * partitions.length * count  / takesMs);

        } finally {
            voter.stop();
        }



    }
    @Ignore
    @Test
    public void multiThreadsWritePerformanceTest() throws IOException, ExecutionException, InterruptedException {
        Voter<byte[], byte[], byte[], byte[]> voter = createVoter();


        try {
            int count = 10 * 1024 * 1024;
            int entrySize = 1024;
            int threads = 1;
            int[] partitions = {2, 3, 4, 5, 6};

            while(voter.voterState() != Voter.VoterState.LEADER) {
                Thread.sleep(50L);
            }

            voter.updateClusterState(new UpdateClusterStateRequest(
                    JdkSerializerFactory.createSerializer(ScalePartitionsEntry.class)
                            .serialize(new ScalePartitionsEntry(partitions)),
                    RaftJournal.RESERVED_PARTITION, 1)).get();


            byte[] entry = ByteUtils.createFixedSizeBytes(entrySize);

            CountDownLatch threadLatch = new CountDownLatch(threads);
            AtomicInteger currentCount = new AtomicInteger(0);

            JMetricFactory factory = ServiceSupport.load(JMetricFactory.class);
            JMetric metric = factory.create("WRITE");


            for (int i = 0; i < threads; i++) {
                final int finalI = i;
                Thread t = new Thread(() -> {
                    int partition = partitions[finalI % partitions.length];
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
        Voter<byte[],byte[], byte[], byte[]> voter = createVoter();


        try {
            byte [] entry = new byte[] {1, 2, 3,5, 8,9};
            while(voter.voterState() != Voter.VoterState.LEADER) {
                Thread.sleep(50L);
            }



            UpdateClusterStateResponse response = voter.updateClusterState(new UpdateClusterStateRequest(entry, RaftJournal.DEFAULT_PARTITION, 1)).get();
            Assert.assertTrue(response.success());
            Assert.assertArrayEquals(entry, response.getResult());

        } finally {
            voter.stop();
        }



    }


    private Voter<byte[], byte[], byte[], byte[]> createVoter() throws IOException {
        StateFactory<byte [], byte [], byte [], byte []> stateFactory = new NoopStateFactory();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4, new NamedThreadFactory("JournalKeeper-Scheduled-Executor"));
        ExecutorService asyncExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, new NamedThreadFactory("JournalKeeper-Async-Executor"));
        BytesSerializer bytesSerializer = new BytesSerializer();
        Properties properties = new Properties();
        properties.setProperty("working_dir", base.toString());
        properties.setProperty("enable_metric", "true");
        properties.setProperty("print_metric_interval_sec", "3");
//        properties.setProperty("cache_requests", String.valueOf(1024L * 1024 * 5));

        Voter<byte [], byte [], byte [], byte []>  voter = new Voter<>(stateFactory, bytesSerializer, bytesSerializer, bytesSerializer, bytesSerializer, scheduledExecutorService, asyncExecutorService, properties);
        URI uri = URI.create("jk://localhost:8888");
        voter.init(uri, Collections.singletonList(uri));
        voter.recover();
        voter.start();
        return voter;
    }

    static class BytesSerializer implements Serializer<byte []> {

        @Override
        public byte[] serialize(byte[] entry) {
            return entry;
        }

        @Override
        public byte[] parse(byte[] bytes) {
            return bytes;
        }
    }


    static class EchoState implements State<byte[], byte[], byte[], byte[]> {
        private AtomicLong lastApplied = new AtomicLong(0L);
        private AtomicInteger term = new AtomicInteger(0);
        @Override
        public byte [] execute(byte[] entry, int partition, long index, int batchSize, Map<String, String> eventParams) {
            return entry;
        }

        @Override
        public long lastApplied() {
            return lastApplied.get();
        }

        @Override
        public int lastIncludedTerm() {
            return term.get();
        }

        @Override
        public void recover(Path path, RaftJournal raftJournal, Properties properties) {
            lastApplied = new AtomicLong(0L);
            term = new AtomicInteger(0);
        }

        @Override
        public State<byte[], byte[], byte[], byte[]> takeASnapshot(Path path, RaftJournal raftJournal) throws IOException {
            return null;
        }

        @Override
        public byte[] readSerializedData(long offset, int size) throws IOException {
            return new byte[0];
        }

        @Override
        public long serializedDataSize() {
            return 0;
        }

        @Override
        public void installSerializedData(byte[] data, long offset) throws IOException {

        }

        @Override
        public void installFinish(long lastApplied, int lastIncludedTerm) {

        }

        @Override
        public void clear() {

        }

        @Override
        public void next() {
            lastApplied.incrementAndGet();
        }

        @Override
        public void skip() {

        }

        @Override
        public CompletableFuture<byte[]> query(byte[] query) {
            return CompletableFuture.supplyAsync(() ->new byte[0]);
        }
    }


    class NoopStateFactory implements StateFactory<byte [], byte [], byte [], byte []> {

        @Override
        public State<byte[], byte[], byte[], byte[]> createState() {
            return new EchoState();
        }
    }
}
