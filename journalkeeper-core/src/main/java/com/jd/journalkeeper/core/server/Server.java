package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.event.EventWatcher;
import com.jd.journalkeeper.core.api.*;
import com.jd.journalkeeper.core.exception.ServiceLoadException;
import com.jd.journalkeeper.core.journal.Journal;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NoSuchSnapshotException;
import com.jd.journalkeeper.persistence.MetadataPersistence;
import com.jd.journalkeeper.persistence.PersistenceAccessPoint;
import com.jd.journalkeeper.persistence.ServerMetadata;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;
import com.jd.journalkeeper.rpc.client.GetServersResponse;
import com.jd.journalkeeper.rpc.client.QueryStateRequest;
import com.jd.journalkeeper.rpc.client.QueryStateResponse;
import com.jd.journalkeeper.rpc.server.*;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.StreamSupport;

/**
 * Server就是集群中的节点，它包含了存储在Server上日志（journal），一组快照（snapshots[]）和一个状态机（stateMachine）实例。
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class Server<E, Q, R>
        extends JournalKeeperServer<E, Q, R>
        implements ServerRpc<E, Q, R>, ClientServerRpc<E, Q, R> {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected final State<E, Q, R> state;

    protected final ScheduledExecutorService scheduledExecutor;

    protected final ExecutorService asyncExecutor;
    /**
     * 心跳间隔、选举超时等随机时间的随机范围
     */
    public final static float RAND_INTERVAL_RANGE = 0.25F;

    /**
     * 所有选民节点地址，包含LEADER
     */
    protected List<URI> voters;

    /**
     * 当前Server URI
     */
    protected URI uri;
    /**
     * 存放日志
     */
    protected Journal<E> journal;
    /**
     * 存放节点上所有状态快照的稀疏数组，数组的索引（key）就是快照对应的日志位置的索引
     */
    protected final NavigableMap<Long, State<E, Q, R>> snapshots = new ConcurrentSkipListMap<>();

    /**
     * 已知的被提交的最大日志条目的索引值（从 0 开始递增）
     */
    protected long commitIndex;

    /**
     * 当前LEADER节点地址
     */
    protected URI leader;

    /**
     * 观察者节点
     */
    protected List<URI> observers;

    /**
     * 每个Server模块中需要运行一个用于执行日志更新状态，保存Snapshot的状态机线程，
     */
    protected final LoopThread stateMachineThread;

    //TODO: Log Compaction
    protected ScheduledFuture flushFuture, compactionFuture;

    /**
     * 可用状态
     */
    private boolean available = false;

    /**
     * 持久化实现接入点
     */
    protected PersistenceAccessPoint persistenceAccessPoint;
    /**
     * 元数据持久化服务
     */
    protected MetadataPersistence metadataPersistence;

    /**
     * 状态读写锁
     * 读取状态是加乐观锁或读锁，变更状态时加写锁。
     */
    protected final StampedLock stateLock = new StampedLock();

    protected void enable(){
        this.available = true;
    }

    protected void disable() {
        this.available = false;
    }

    protected boolean isAvailable() {
        return available;
    }

    private Config config;

    public Server(StateFactory<E, Q, R> stateFactory,  ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties){
        super(stateFactory, properties);
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.config = toConfig(properties);
        this.stateMachineThread = buildStateMachineThread();
        this.state = stateFactory.createState();
        persistenceAccessPoint = StreamSupport.
                stream(ServiceLoader.load(PersistenceAccessPoint.class).spliterator(), false)
                .findFirst().orElseThrow(ServiceLoadException::new);
        metadataPersistence = persistenceAccessPoint.getMetadataPersistence(properties);
    }

    private LoopThread buildStateMachineThread() {
        return LoopThread.builder()
                .name(String.format("StateMachineThread-%s", uri.toString()))
                .doWork(this::applyEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("StateMachineThread Exception: ", e))
                .build();
    }

    @Override
    public void init(URI uri, List<URI> voters) {
        this.uri = uri;
        this.voters = voters;
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setSnapshotStep(Integer.parseInt(
                properties.getProperty(
                        Config.SNAPSHOT_STEP_KEY,
                        String.valueOf(Config.DEFAULT_SNAPSHOT_STEP))));
        config.setRpcTimeoutMs(Long.parseLong(
                properties.getProperty(
                        Config.RPC_TIMEOUT_MS_KEY,
                        String.valueOf(Config.DEFAULT_RPC_TIMEOUT_MS))));
        config.setFlushIntervalMs(Long.parseLong(
                properties.getProperty(
                        Config.FLUSH_INTERVAL_MS_KEY,
                        String.valueOf(Config.DEFAULT_FLUSH_INTERVAL_MS))));

        config.setWorkingDir(Paths.get(
                properties.getProperty(Config.WORKING_DIR_KEY,
                        config.getWorkingDir().normalize().toString())));

        config.setGetStateBatchSize(Long.parseLong(
                properties.getProperty(
                        Config.GET_STATE_BATCH_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_GET_STATE_BATCH_SIZE))));

        return config;
    }

    protected Path workingDir() {
        return config.getWorkingDir();
    }

    protected Path snapshotsPath() {
        return workingDir().resolve("snapshots");
    }

    protected Path statePath() {
        return workingDir().resolve("state");
    }

    /**
     * 监听属性commitIndex的变化，
     * 当commitIndex变更时如果commitIndex > lastApplied，
     * 反复执行applyEntries直到lastApplied == commitIndex：
     *
     * 1. 如果需要，复制当前状态为新的快照保存到属性snapshots, 索引值为lastApplied。
     * 2. lastApplied自增，将log[lastApplied]应用到状态机，更新当前状态state；
     *
     */
    private void applyEntries() {
        while ( state.lastApplied() < commitIndex) {
            takeASnapShotIfNeed();
            E entry = journal.read(state.lastApplied());
            long stamp = stateLock.writeLock();
            try {
                state.execute(entry);
            } finally {
                stateLock.unlockWrite(stamp);
            }
            CompletableFuture.runAsync(this::onStateChanged);
        }
    }


    /**
     * 当状态变化时触发事件
     */
    protected void onStateChanged() {}

    /**
     * 如果需要，保存一次快照
     */
    private void takeASnapShotIfNeed() {
        if(snapshots.isEmpty() || state.lastApplied() - snapshots.lastKey() > config.getSnapshotStep()) {
            State<E, Q, R> snapshot = state.takeASnapshot(snapshotsPath().resolve(String.valueOf(state.lastApplied())));
            snapshots.put(snapshot.lastApplied(), snapshot);
        }
    }

    @Override
    public CompletableFuture<QueryStateResponse<R>> queryServerState(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                QueryStateResponse<R> response;
                long stamp = stateLock.tryOptimisticRead();
                response = new QueryStateResponse<>(state.query(request.getQuery()).get(), state.lastApplied());
                if(!stateLock.validate(stamp)) {
                    stamp = stateLock.readLock();
                    try {
                        response = new QueryStateResponse<>(state.query(request.getQuery()).get(), state.lastApplied());

                    } finally {
                        stateLock.unlockRead(stamp);
                    }
                }
                return response;
            } catch (Throwable throwable) {
                return new QueryStateResponse<>(throwable);
            }
        });
    }

    /**
     * 如果请求位置存在对应的快照，直接从快照中读取状态返回；如果请求位置不存在对应的快照，那么需要找到最近快照日志，以这个最近快照日志对应的快照为输入，从最近快照日志开始（不含）直到请求位置（含）依次在状态机中执行这些日志，执行完毕后得到的快照就是请求位置的对应快照，读取这个快照的状态返回给客户端即可。
     * 实现流程：
     *
     * 对比logIndex与在属性snapshots数组的上下界，检查请求位置是否越界，如果越界返回INDEX_OVERFLOW/INDEX_UNDERFLOW错误。
     * 查询snapshots[logIndex]是否存在，如果存在快照中读取状态返回，否则下一步；
     * 找到snapshots中距离logIndex最近且小于logIndex的快照位置和快照，记为nearestLogIndex和nearestSnapshot；
     * 从log中的索引位置nearestLogIndex + 1开始，读取N条日志，N = logIndex - nearestLogIndex获取待执行的日志数组execLogs[]；
     * 调用以nearestSnapshot为输入，依次在状态机stateMachine中执行execLogs，得到logIndex位置对应的快照，从快照中读取状态返回。
     */
    @Override
    public CompletableFuture<QueryStateResponse<R>> querySnapshot(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> {

            try {


                if (request.getIndex() > state.lastApplied()) {
                    throw new IndexOverflowException();
                }


                long stamp = stateLock.tryOptimisticRead();
                if (request.getIndex() == state.lastApplied()) {
                    try {
                        return new QueryStateResponse<>(state.query(request.getQuery()).get(), state.lastApplied());
                    } catch (Throwable throwable) {
                        return new QueryStateResponse<>(throwable);
                    }
                }
                if(!stateLock.validate(stamp)) {
                    stamp = stateLock.readLock();
                    try {
                        if (request.getIndex() == state.lastApplied()) {
                            try {
                                return new QueryStateResponse<>(state.query(request.getQuery()).get(), state.lastApplied());
                            } catch (Throwable throwable) {
                                return new QueryStateResponse<>(throwable);
                            }
                        }
                    } finally {
                        stateLock.unlockRead(stamp);
                    }
                }


                State<E, Q, R> requestState = snapshots.get(request.getIndex());

                if (null == requestState) {
                    Map.Entry<Long, State<E, Q, R>> nearestSnapshot = snapshots.floorEntry(request.getIndex());
                    if (null == nearestSnapshot) {
                        throw new IndexUnderflowException();
                    }

                    E[] toBeExecutedEntries = journal.read(nearestSnapshot.getKey(), (int) (request.getIndex() - nearestSnapshot.getKey()));
                    Path tempSnapshotPath = snapshotsPath().resolve(String.valueOf(request.getIndex()));
                    if(Files.exists(tempSnapshotPath)) {
                        throw new ConcurrentModificationException(String.format("A snapshot of position %d is creating, please retry later.", request.getIndex()));
                    }
                    requestState = state.takeASnapshot(tempSnapshotPath);
                    for(E entry: toBeExecutedEntries) {
                        requestState.execute(entry);
                    }
                    if(requestState instanceof Flushable) {
                        ((Flushable ) requestState).flush();
                    }
                    snapshots.putIfAbsent(request.getIndex(), requestState);
                }
                return new QueryStateResponse<>(requestState.query(request.getQuery()).get());
            } catch (Throwable throwable) {
                return new QueryStateResponse<>(throwable);
            }
        });
    }

    @Override
    public CompletableFuture<GetServersResponse> getServer() {
        return CompletableFuture.supplyAsync(() -> new GetServersResponse(new ClusterConfiguration(leader, voters, observers)));
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            if(!snapshots.isEmpty()) {

                long snapshotIndex = request.getLastIncludedIndex() + 1;
                if (snapshotIndex < 0) {
                    snapshotIndex = snapshots.lastKey();
                }
                State<E, Q, R> state = snapshots.get(snapshotIndex);
                if (null != state) {
                    byte [] data = state.readSerializedData(request.getOffset(),config.getGetStateBatchSize());
                    return new GetServerStateResponse(
                            state.lastIncludedIndex(), state.lastIncludedTerm(),
                            request.getOffset(),
                            data,
                            request.getOffset() + data.length >= state.serializedDataSize());
                }
            }
            return new GetServerStateResponse(new NoSuchSnapshotException());
        }).exceptionally(GetServerStateResponse::new);
    }

    @Override
    public void start() {
        stateMachineThread.start();
        flushFuture = scheduledExecutor.scheduleAtFixedRate(this::flush,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getFlushIntervalMs(), TimeUnit.MILLISECONDS);

    }

    /**
     * 刷盘：
     * 1. 日志
     * 2. 状态
     * 3. 元数据
     */
    private void flush() {
        //FIXME: 如果刷盘异常，如何保证日志、状态和元数据三者一致？
        try {
            journal.flush();
            if(state instanceof Flushable) {
                ((Flushable) state).flush();
            }
            metadataPersistence.save(createServerMetadata());
        } catch (IOException e) {
            logger.warn("Flush exception: ", e);
        }
    }

    @Override
    public void stop() {
        try {
            stateMachineThread.stop();
            stopAndWaitScheduledFeature(flushFuture, 1000L);
        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception: ", t);
        }
    }

    protected void stopAndWaitScheduledFeature(ScheduledFuture scheduledFuture, long timeout) throws TimeoutException {
        if (scheduledFuture != null) {
            long t0 = System.currentTimeMillis();
            while (!scheduledFuture.isDone()) {
                if(System.currentTimeMillis() - t0 > timeout) {
                    throw new TimeoutException("Wait for async job timeout!");
                }
                scheduledFuture.cancel(true);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    logger.warn("Exception: ", e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<JournalKeeperClient<Q, R, E>> connect(Set<URI> servers, Properties properties) {
        // TODO
        return null;
    }


    @Override
    public void watch(EventWatcher eventWatcher) {
        // TODO
    }

    @Override
    public void unwatch(EventWatcher eventWatcher) {
        // TODO
    }


    /**
     * 从持久化存储恢复
     * 1. 元数据
     * 2. 状态和快照
     * 3. 日志
     */
    @Override
    public void recover() throws IOException {
        onMetadataRecovered(metadataPersistence.load());
        state.init(statePath(), properties);
        recoverSnapshots();
        recoverJournal();
    }

    private void recoverJournal() {
        // TODO
    }

    private void recoverSnapshots() throws IOException {
        StreamSupport.stream(
                Files.newDirectoryStream(snapshotsPath(),
                        entry -> entry.getFileName().toString().matches("\\d+")
                    ).spliterator(), false)
                .map(path -> {
                    State<E, Q, R> snapshot = stateFactory.createState();
                    snapshot.init(path, properties);
                    if(Long.parseLong(path.getFileName().toString()) == snapshot.lastApplied()) {
                        return snapshot;
                    } else {
                        if(snapshot instanceof Closeable) {
                            try {
                                ((Closeable) snapshot).close();
                            } catch (IOException e) {
                                logger.warn("Exception: ", e);
                            }
                        }
                        return null;
                    }
                }).filter(Objects::nonNull)
                .forEach(snapshot -> snapshots.put(snapshot.lastApplied(), snapshot));
    }

    protected void onMetadataRecovered(ServerMetadata metadata) {
        this.uri = metadata.getThisServer();
        this.commitIndex = metadata.getCommitIndex();
    }

    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = new ServerMetadata();
        serverMetadata.setThisServer(uri);
        serverMetadata.setCommitIndex(commitIndex);
        return serverMetadata;
    }

    protected long randomInterval(long interval) {
        return interval + Math.round(ThreadLocalRandom.current().nextDouble(-1 * RAND_INTERVAL_RANGE, RAND_INTERVAL_RANGE) * interval);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse<StorageEntry<E>>> getServerEntries(GetServerEntriesRequest request) {
        return CompletableFuture.supplyAsync(() ->
                new GetServerEntriesResponse<>(
                        journal.readRaw(request.getIndex(), request.getMaxSize()),
                        journal.minIndex(), state.lastApplied()));
    }

    @Override
    public ServerState serverState() {
        return stateMachineThread.serverState();
    }

    protected long getRpcTimeoutMs() {
        return config.getRpcTimeoutMs();
    }


    static class Config {
        final static int DEFAULT_SNAPSHOT_STEP = 128;
        final static long DEFAULT_RPC_TIMEOUT_MS = 1000L;
        final static long DEFAULT_FLUSH_INTERVAL_MS = 50L;
        final static long DEFAULT_GET_STATE_BATCH_SIZE = 1024 * 1024;

        final static String SNAPSHOT_STEP_KEY = "snapshot_step";
        final static String RPC_TIMEOUT_MS_KEY = "rpc_timeout_ms";
        final static String FLUSH_INTERVAL_MS_KEY = "flush_interval_ms";
        final static String WORKING_DIR_KEY = "working_dir";
        final static String GET_STATE_BATCH_SIZE_KEY = "get_state_batch_size";

        private int snapshotStep = DEFAULT_SNAPSHOT_STEP;
        private long rpcTimeoutMs = DEFAULT_RPC_TIMEOUT_MS;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private Path workingDir = Paths.get(System.getProperty("user.dir"));
        private long getStateBatchSize = DEFAULT_GET_STATE_BATCH_SIZE;

        int getSnapshotStep() {
            return snapshotStep;
        }

        void setSnapshotStep(int snapshotStep) {
            this.snapshotStep = snapshotStep;
        }

        long getRpcTimeoutMs() {
            return rpcTimeoutMs;
        }

        void setRpcTimeoutMs(long rpcTimeoutMs) {
            this.rpcTimeoutMs = rpcTimeoutMs;
        }

        public long getFlushIntervalMs() {
            return flushIntervalMs;
        }

        public void setFlushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
        }

        public Path getWorkingDir() {
            return workingDir;
        }

        public void setWorkingDir(Path workingDir) {
            this.workingDir = workingDir;
        }

        public long getGetStateBatchSize() {
            return getStateBatchSize;
        }

        public void setGetStateBatchSize(long getStateBatchSize) {
            this.getStateBatchSize = getStateBatchSize;
        }
    }
}
