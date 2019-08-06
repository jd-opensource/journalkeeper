package io.journalkeeper.core;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntrySerializer;
import io.journalkeeper.core.server.Voter;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.test.ByteUtils;
import io.journalkeeper.utils.test.TestPathUtils;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
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
    public void singleNodeServerPerformanceTest() throws IOException, ExecutionException, InterruptedException {
        StateFactory<byte [], byte [], byte []> stateFactory = new NoopStateFactory();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(4, new NamedThreadFactory("JournalKeeper-Scheduled-Executor"));
        ExecutorService asyncExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, new NamedThreadFactory("JournalKeeper-Async-Executor"));
        BytesSerializer bytesSerializer = new BytesSerializer();
        Properties properties = new Properties();
        properties.setProperty("working_dir", base.toString());
//        properties.setProperty("cache_requests", String.valueOf(1024L * 1024 * 5));

        Voter<byte [], byte [], byte []>  voter = new Voter<>(stateFactory, bytesSerializer, bytesSerializer, bytesSerializer, scheduledExecutorService, asyncExecutorService, properties);
        URI uri = URI.create("jk://localhost:8888");
        voter.init(uri, Collections.singletonList(uri));
        voter.recover();
        voter.start();




        try {
            long count = 5 * 1024 * 1024;
            int entrySize = 1024;
            int[] partitions = {2};

            while(voter.voterState() != Voter.VoterState.LEADER) {
                Thread.sleep(50L);
            }

            voter.updateClusterState(new UpdateClusterStateRequest(new ScalePartitionsEntrySerializer().serialize(new ScalePartitionsEntry(partitions)),
                    RaftJournal.RESERVED_PARTITION, 1)).get();


            byte[] entry = ByteUtils.createFixedSizeBytes(entrySize);
            long t0 = System.nanoTime();

            for (int i = 0; i < partitions.length; i++) {
                UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry, partitions[i], 1, ResponseConfig.REPLICATION);

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

    static class BytesSerializer implements Serializer<byte []> {
        @Override
        public int sizeOf(byte[] bytes) {
            return bytes.length;
        }

        @Override
        public byte[] serialize(byte[] entry) {
            return entry;
        }

        @Override
        public byte[] parse(byte[] bytes) {
            return bytes;
        }
    }


    static class NoopState implements State<byte[], byte[], byte[]> {
        private AtomicLong lastApplied = new AtomicLong(0L);
        private AtomicInteger term = new AtomicInteger(0);
        @Override
        public Map<String, String> execute(byte[] entry, int partition, long index, int batchSize) {
            return null;
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
        public State<byte[], byte[], byte[]> takeASnapshot(Path path, RaftJournal raftJournal) throws IOException {
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


    class NoopStateFactory implements StateFactory<byte [], byte [], byte []> {

        @Override
        public State<byte[], byte[], byte[]> createState() {
            return new NoopState();
        }
    }
}
