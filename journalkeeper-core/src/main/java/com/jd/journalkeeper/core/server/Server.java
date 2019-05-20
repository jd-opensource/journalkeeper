package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.entry.reserved.CompactJournalEntrySerializer;
import com.jd.journalkeeper.core.entry.reserved.ReservedEntry;
import com.jd.journalkeeper.core.entry.reserved.ScalePartitionsEntrySerializer;
import com.jd.journalkeeper.core.journal.Journal;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.utils.event.*;
import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.entry.Entry;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NoSuchSnapshotException;
import com.jd.journalkeeper.persistence.BufferPool;
import com.jd.journalkeeper.persistence.MetadataPersistence;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.persistence.ServerMetadata;
import com.jd.journalkeeper.rpc.RpcAccessPointFactory;
import com.jd.journalkeeper.rpc.server.*;
import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.state.StateServer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.jd.journalkeeper.core.api.RaftJournal.RESERVED_PARTITION;

/**
 * Server就是集群中的节点，它包含了存储在Server上日志（journal），一组快照（snapshots[]）和一个状态机（stateMachine）实例。
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class Server<E, Q, R>
        extends RaftServer<E, Q, R>
        implements ServerRpc {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public URI serverUri() {
        return uri;
    }

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
    protected Journal journal;
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

    //TODO: Log Compaction, install snapshot rpc
    protected ScheduledFuture flushFuture, compactionFuture;

    /**
     * 可用状态
     */
    private boolean available = false;

    /**
     * 持久化实现接入点
     */
    protected PersistenceFactory persistenceFactory;
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

    protected final Serializer<E> entrySerializer;
    protected final Serializer<Q> querySerializer;
    protected final Serializer<R> resultSerializer;

    protected final BufferPool bufferPool;

    protected ServerRpcAccessPoint serverRpcAccessPoint;
    protected final RpcAccessPointFactory rpcAccessPointFactory;
    protected StateServer rpcServer = null;

    protected final Map<URI, ServerRpc> remoteServers = new HashMap<>();
    private Config config;

    private ServerState serverState = ServerState.STOPPED;
    private ServerMetadata lastSavedServerMetadata = null;
    private AtomicBoolean flushGate = new AtomicBoolean(false);
    protected final EventBus eventBus;
    private final CompactJournalEntrySerializer compactJournalEntrySerializer = new CompactJournalEntrySerializer();
    private final ScalePartitionsEntrySerializer scalePartitionsEntrySerializer = new ScalePartitionsEntrySerializer();
    protected final Lock scalePartitionLock = new ReentrantLock(true);
    protected final Lock compactLock = new ReentrantLock(true);

    public Server(StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, Serializer<Q> querySerializer,
                  Serializer<R> resultSerializer, ScheduledExecutorService scheduledExecutor,
                  ExecutorService asyncExecutor, Properties properties){
        super(stateFactory, properties);
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.config = toConfig(properties);
        this.stateMachineThread = buildStateMachineThread();
        this.state = stateFactory.createState();
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.eventBus = new EventBus(asyncExecutor, config.getRpcTimeoutMs());
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();
        bufferPool = ServiceSupport.load(BufferPool.class);
        rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);
        journal = new Journal(
                persistenceFactory,
                bufferPool);

    }

    private LoopThread buildStateMachineThread() {
        return LoopThread.builder()
                .name("StateMachineThread")
                .condition(() ->this.serverState() == ServerState.RUNNING)
                .doWork(this::applyEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("StateMachineThread Exception: ", e))
                .daemon(true)
                .build();
    }

    @Override
    public void init(URI uri, List<URI> voters) throws IOException {
        this.uri = uri;
        this.voters = voters;

        createMissingDirectories();

        metadataPersistence.recover(metadataPath(), properties);
        lastSavedServerMetadata = createServerMetadata();
        metadataPersistence.save(lastSavedServerMetadata);

    }

    private void createMissingDirectories() throws IOException {
        Files.createDirectories(metadataPath());
        Files.createDirectories(statePath());
        Files.createDirectories(snapshotsPath());
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

        config.setGetStateBatchSize(Integer.parseInt(
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
    protected Path metadataPath() {
        return workingDir().resolve("metadata");
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
    private void applyEntries()  {
        while ( state.lastApplied() < commitIndex) {
            takeASnapShotIfNeed();
            Entry storageEntry = journal.read(state.lastApplied());
            Map<String, String> customizedEventData = null;
            if(storageEntry.getHeader().getPartition() != RESERVED_PARTITION) {
                E entry = entrySerializer.parse(storageEntry.getEntry());
                long stamp = stateLock.writeLock();
                try {
                    customizedEventData = state.execute(entry, storageEntry.getHeader().getPartition(), state.lastApplied());
                } finally {
                    stateLock.unlockWrite(stamp);
                }
            } else {
                applyReservedEntry(storageEntry.getEntry()[0], storageEntry.getEntry());
            }
            state.next();
            asyncExecutor.submit(this::onStateChanged);
            Map<String, String> parameters = new HashMap<>(customizedEventData == null ? 1: customizedEventData.size() + 1);
            if(null != customizedEventData) {
                customizedEventData.forEach(parameters::put);
            }
            parameters.put("lastApplied", String.valueOf(state.lastApplied()));
            fireEvent(EventType.ON_STATE_CHANGE, parameters);
        }
    }

    protected void applyReservedEntry(int type, byte [] reservedEntry) {
        switch (type) {
            case ReservedEntry
                    .TYPE_LEADER_ANNOUNCEMENT:
                // Nothing to do.
                break;
            case ReservedEntry.TYPE_COMPACT_JOURNAL:
                compactJournalAsync(compactJournalEntrySerializer.parse(reservedEntry).getCompactIndices());
                break;
            case ReservedEntry.TYPE_SCALE_PARTITIONS:
                scalePartitionsAsync(scalePartitionsEntrySerializer.parse(reservedEntry).getPartitions());
                break;
            default:
                logger.warn("Invalid reserved entry type: {}.", type);
        }
    }

    private void scalePartitionsAsync(int[] partitions) {
        asyncExecutor.submit(() -> {
            try {
                scalePartitionLock.lock();
                journal.rePartition(Arrays.stream(partitions).boxed().collect(Collectors.toSet()));
            } finally {
                scalePartitionLock.unlock();
            }

        });
    }

    private void compactJournalAsync(Map<Integer, Long> compactIndices) {
        asyncExecutor.submit(() -> {
            try {
                compactLock.lock();
                journal.compactByPartition(compactIndices);
            } catch (IOException e) {
                logger.warn("Compact journal exception: ", e);
            } finally {
                compactLock.unlock();
            }

        });
    }

    protected void fireEvent(int eventType, Map<String, String> eventData) {
        eventBus.fireEvent(new Event(eventType, eventData));
    }


    /**
     * 当状态变化时触发事件
     */
    protected void onStateChanged() {}

    /**
     * 如果需要，保存一次快照
     */
    private void takeASnapShotIfNeed() {
        asyncExecutor.submit(() -> {
            try {
                synchronized (snapshots) {
                    if (config.getSnapshotStep() > 0 && (snapshots.isEmpty() || state.lastApplied() - snapshots.lastKey() > config.getSnapshotStep())) {

                        State<E, Q, R> snapshot = state.takeASnapshot(snapshotsPath().resolve(String.valueOf(state.lastApplied())), journal);
                        snapshots.put(snapshot.lastApplied(), snapshot);
                    }
                }
            } catch (IOException e) {
                logger.warn("Take snapshot exception: ", e);
            }
        });
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                QueryStateResponse response;
                long stamp = stateLock.tryOptimisticRead();
                response = new QueryStateResponse(
                        resultSerializer.serialize(
                            state.query(querySerializer.parse(request.getQuery())).get()),
                        state.lastApplied());
                if(!stateLock.validate(stamp)) {
                    stamp = stateLock.readLock();
                    try {
                        response = new QueryStateResponse(
                                resultSerializer.serialize(
                                        state.query(querySerializer.parse(request.getQuery())).get()),
                                state.lastApplied());

                    } finally {
                        stateLock.unlockRead(stamp);
                    }
                }
                return response;
            } catch (Throwable throwable) {
                return new QueryStateResponse(throwable);
            }
        }, asyncExecutor);
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
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {

            try {
                if (request.getIndex() > state.lastApplied()) {
                    throw new IndexOverflowException();
                }


                long stamp = stateLock.tryOptimisticRead();
                if (request.getIndex() == state.lastApplied()) {
                    try {
                        return new QueryStateResponse(
                                resultSerializer.serialize(
                                        state.query(querySerializer.parse(request.getQuery())).get()),
                                state.lastApplied());
                    } catch (Throwable throwable) {
                        return new QueryStateResponse(throwable);
                    }
                }
                if(!stateLock.validate(stamp)) {
                    stamp = stateLock.readLock();
                    try {
                        if (request.getIndex() == state.lastApplied()) {
                            try {
                                return new QueryStateResponse(
                                        resultSerializer.serialize(
                                                state.query(querySerializer.parse(request.getQuery())).get()),
                                        state.lastApplied());
                            } catch (Throwable throwable) {
                                return new QueryStateResponse(throwable);
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

                    List<RaftEntry> toBeExecutedEntries = new ArrayList<>(journal.batchRead(nearestSnapshot.getKey(), (int) (request.getIndex() - nearestSnapshot.getKey())));
                    Path tempSnapshotPath = snapshotsPath().resolve(String.valueOf(request.getIndex()));
                    if(Files.exists(tempSnapshotPath)) {
                        throw new ConcurrentModificationException(String.format("A snapshot of position %d is creating, please retry later.", request.getIndex()));
                    }
                    requestState = state.takeASnapshot(tempSnapshotPath, journal);
                    for (int i = 0; i < toBeExecutedEntries.size(); i++) {
                        RaftEntry entry = toBeExecutedEntries.get(i);
                        requestState.execute(entrySerializer.parse(entry.getEntry()), entry.getHeader().getPartition(), nearestSnapshot.getKey() + i);
                    }
                    if(requestState instanceof Flushable) {
                        ((Flushable ) requestState).flush();
                    }
                    snapshots.putIfAbsent(request.getIndex(), requestState);
                }
                return new QueryStateResponse(resultSerializer.serialize(requestState.query(querySerializer.parse(request.getQuery())).get()));
            } catch (Throwable throwable) {
                return new QueryStateResponse(throwable);
            }
        }, asyncExecutor);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        this.eventBus.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        this.eventBus.unWatch(eventWatcher);
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return CompletableFuture.supplyAsync(() ->
                new AddPullWatchResponse(eventBus.addPullWatch(), eventBus.pullIntervalMs()), asyncExecutor);
    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return CompletableFuture
                .runAsync(() -> eventBus.removePullWatch(request.getPullWatchId()), asyncExecutor)
                .thenApply(v -> new RemovePullWatchResponse());
    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            if(request.getAckSequence() >= 0 ) {
                eventBus.ackPullEvents(request.getPullWatchId(), request.getAckSequence());
            }
            return new PullEventsResponse(eventBus.pullEvents(request.getPullWatchId()));
        }, asyncExecutor);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return CompletableFuture.supplyAsync(() -> new GetServersResponse(new ClusterConfiguration(leader, voters, observers)), asyncExecutor);
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
                    byte [] data;
                    try {
                        data = state.readSerializedData(request.getOffset(),config.getGetStateBatchSize());
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                    return new GetServerStateResponse(
                            state.lastIncludedIndex(), state.lastIncludedTerm(),
                            request.getOffset(),
                            data,
                            request.getOffset() + data.length >= state.serializedDataSize());
                }
            }
            return new GetServerStateResponse(new NoSuchSnapshotException());
        }, asyncExecutor).exceptionally(GetServerStateResponse::new);
    }

    @Override
    public final void start() {
        this.serverState = ServerState.STARTING;
        doStart();
        stateMachineThread.start();
        flushFuture = scheduledExecutor.scheduleAtFixedRate(this::flush,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
        rpcServer = rpcAccessPointFactory.bindServerService(this);
        rpcServer.start();
        this.serverState = ServerState.RUNNING;
    }

    protected abstract void doStart();
    /**
     * 刷盘：
     * 1. 日志
     * 2. 状态
     * 3. 元数据
     */
    @Override
    public boolean flush() {
        //FIXME: 如果刷盘异常，如何保证日志、状态和元数据三者一致？
        if (flushGate.compareAndSet(false, true)) {
            try {

                journal.flush();
                if (state instanceof Flushable) {
                    ((Flushable) state).flush();
                }
                ServerMetadata serverMetadata = createServerMetadata();
                if (!serverMetadata.equals(lastSavedServerMetadata)) {
                    metadataPersistence.save(serverMetadata);
                    lastSavedServerMetadata = serverMetadata;
                }
            } catch(IOException e){
                logger.warn("Flush exception, commitIndex: {}, lastApplied: {}, server: {}: ",
                       commitIndex, state.lastApplied(), uri ,e);
            } finally {
                flushGate.set(false);
            }
            return true;
        } else {
            return false;
        }
    }

    protected ServerRpc getServerRpc(URI uri) {
        return remoteServers.computeIfAbsent(uri, uri1 -> serverRpcAccessPoint.getServerRpcAgent(uri1));
    }

    @Override
    public final void stop() {
        try {
            this.serverState = ServerState.STOPPING;
            doStop();
            remoteServers.values().forEach(ServerRpc::stop);
            if(rpcServer != null) {
                rpcServer.stop();
            }
            serverRpcAccessPoint.stop();
            stateMachineThread.stop();
            stopAndWaitScheduledFeature(flushFuture, 1000L);
            if(persistenceFactory instanceof Closeable) {
                ((Closeable) persistenceFactory).close();
            }
            this.serverState = ServerState.STOPPED;
        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception: ", t);
        }
    }
    protected abstract void doStop();
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

    /**
     * 从持久化存储恢复
     * 1. 元数据
     * 2. 状态和快照
     * 3. 日志
     */
    @Override
    public void recover() throws IOException {
        lastSavedServerMetadata = metadataPersistence.recover(metadataPath(), properties);
        onMetadataRecovered(lastSavedServerMetadata);
        recoverJournal(lastSavedServerMetadata.getPartitions());
        state.recover(statePath(), journal, properties);
        recoverSnapshots();
    }


    private void recoverJournal(Set<Integer> partitions) throws IOException {
        journal.recover(journalPath(), properties);
        journal.rePartition(partitions);
    }

    private Path journalPath() {
        return workingDir().resolve("journal");
    }
    private void recoverSnapshots() throws IOException {
        if(!Files.isDirectory(snapshotsPath())) {
            Files.createDirectories(snapshotsPath());
        }
        StreamSupport.stream(
                Files.newDirectoryStream(snapshotsPath(),
                        entry -> entry.getFileName().toString().matches("\\d+")
                    ).spliterator(), false)
                .map(path -> {
                    State<E, Q, R> snapshot = stateFactory.createState();
                    snapshot.recover(path, journal, properties);
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

        if(metadata.getPartitions() == null ) {
            metadata.setPartitions(new HashSet<>());
        }

        if(metadata.getPartitions().isEmpty()) {
            metadata.getPartitions().addAll(
                    Stream.of(RaftJournal.DEFAULT_PARTITION, RESERVED_PARTITION)
                            .collect(Collectors.toSet())
            );
        }

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
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return CompletableFuture.supplyAsync(() ->
                new GetServerEntriesResponse(
                        journal.readRaw(request.getIndex(), request.getMaxSize()),
                        journal.minIndex(), state.lastApplied()), asyncExecutor);
    }

    @Override
    public ServerState serverState() {
        return this.serverState;
    }

    protected long getRpcTimeoutMs() {
        return config.getRpcTimeoutMs();
    }

    public void compact(long indexExclusive) throws IOException {
        if(config.getSnapshotStep() > 0) {
            Map.Entry<Long, State<E, Q, R>> nearestEntry = snapshots.floorEntry(indexExclusive);
            SortedMap<Long, State<E, Q, R>> toBeRemoved = snapshots.headMap(nearestEntry.getKey());
            while (!toBeRemoved.isEmpty()) {
                toBeRemoved.remove(toBeRemoved.firstKey()).clear();
            }
            journal.compact(nearestEntry.getKey());
        } else {
            journal.compact(indexExclusive);
        }
    }

    public State<E, Q, R> getState() {
        return state;
    }

    static class Config {
        final static int DEFAULT_SNAPSHOT_STEP = 128;
        final static long DEFAULT_RPC_TIMEOUT_MS = 1000L;
        final static long DEFAULT_FLUSH_INTERVAL_MS = 50L;
        final static int DEFAULT_GET_STATE_BATCH_SIZE = 1024 * 1024;

        final static String SNAPSHOT_STEP_KEY = "snapshot_step";
        final static String RPC_TIMEOUT_MS_KEY = "rpc_timeout_ms";
        final static String FLUSH_INTERVAL_MS_KEY = "flush_interval_ms";
        final static String WORKING_DIR_KEY = "working_dir";
        final static String GET_STATE_BATCH_SIZE_KEY = "get_state_batch_size";

        private int snapshotStep = DEFAULT_SNAPSHOT_STEP;
        private long rpcTimeoutMs = DEFAULT_RPC_TIMEOUT_MS;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private Path workingDir = Paths.get(System.getProperty("user.dir")).resolve("journalkeeper");
        private int getStateBatchSize = DEFAULT_GET_STATE_BATCH_SIZE;

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

        public int getGetStateBatchSize() {
            return getStateBatchSize;
        }

        public void setGetStateBatchSize(int getStateBatchSize) {
            this.getStateBatchSize = getStateBatchSize;
        }
    }
}
