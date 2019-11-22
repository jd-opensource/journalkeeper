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
package io.journalkeeper.core.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.core.entry.internal.*;
import io.journalkeeper.core.exception.JournalException;
import io.journalkeeper.core.exception.RecoverException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.journal.JournalSnapshot;
import io.journalkeeper.core.metric.DummyMetric;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.JournalKeeperState;
import io.journalkeeper.core.state.StateQueryResult;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NoSuchSnapshotException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.metric.JMetricFactory;
import io.journalkeeper.metric.JMetricSupport;
import io.journalkeeper.persistence.BufferPool;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.persistence.PersistenceFactory;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.client.AddPullWatchResponse;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.ConvertRollResponse;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.PullEventsRequest;
import io.journalkeeper.rpc.client.PullEventsResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.RemovePullWatchRequest;
import io.journalkeeper.rpc.client.RemovePullWatchResponse;
import io.journalkeeper.rpc.server.GetServerEntriesRequest;
import io.journalkeeper.rpc.server.GetServerEntriesResponse;
import io.journalkeeper.rpc.server.GetServerStateRequest;
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import io.journalkeeper.utils.threads.Threads;
import io.journalkeeper.utils.threads.ThreadsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;
import static io.journalkeeper.core.server.ThreadNames.FLUSH_JOURNAL_THREAD;
import static io.journalkeeper.core.server.ThreadNames.PRINT_METRIC_THREAD;
import static io.journalkeeper.core.server.ThreadNames.STATE_MACHINE_THREAD;

// TODO: Add an UUID for each server instance,
//  so multiple server instance of one process
//  can be identified by UUID in logs and threads dump.

/**
 * Server就是集群中的节点，它包含了存储在Server上日志（journal），一组快照（snapshots[]）和一个状态机（stateMachine）实例。
 * @author LiYue
 * Date: 2019-03-14
 */
public abstract class AbstractServer<E, ER, Q, QR>
        implements ServerRpc, RaftServer, ServerRpcProvider, MetricProvider {
    private static final Logger logger = LoggerFactory.getLogger(AbstractServer.class);

    private static final String STATE_PATH = "state";
    private static final String SNAPSHOTS_PATH = "snapshots";
    private static final String METADATA_PATH = "metadata";
    private static final String METADATA_FILE = "metadata";

    @Override
    public URI serverUri() {
        return uri;
    }

    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected final JournalKeeperState<E, ER, Q, QR> state;

    protected final ScheduledExecutorService scheduledExecutor;

    protected final ExecutorService asyncExecutor;
    /**
     * 心跳间隔、选举超时等随机时间的随机范围
     */
    public final static float RAND_INTERVAL_RANGE = 0.5F;

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
    protected final NavigableMap<Long, JournalKeeperState<E, ER, Q, QR>> snapshots = new ConcurrentSkipListMap<>();

    /**
     * 当前LEADER节点地址
     */
    protected URI leaderUri;

    /**
     * 观察者节点
     */
    protected List<URI> observers;
    protected ScheduledFuture flushStateFuture;

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
    protected final Serializer<ER> entryResultSerializer;
    protected final Serializer<Q> querySerializer;
    protected final Serializer<QR> resultSerializer;

    protected final BufferPool bufferPool;

    protected ServerRpcAccessPoint serverRpcAccessPoint;

    protected final Map<URI, ServerRpc> remoteServers = new HashMap<>();
    private Config config;

    private volatile ServerState serverState = ServerState.CREATED;
    /**
     * 上次保存的元数据
     */
    private ServerMetadata lastSavedServerMetadata = null;

    protected final EventBus eventBus;

    protected final Threads threads = ThreadsFactory.create();

    private final JMetricFactory metricFactory;
    private final Map<String, JMetric> metricMap;
    private final static JMetric DUMMY_METRIC = new DummyMetric();

    private final static String METRIC_APPLY_ENTRIES = "APPLY_ENTRIES";

    private final JMetric applyEntriesMetric;

    protected final Properties properties;
    protected final StateFactory<E, ER, Q, QR> stateFactory;
    protected final JournalEntryParser journalEntryParser;
    protected final VoterConfigManager voterConfigManager;
    protected final PartialSnapshot partialSnapshot = new PartialSnapshot();

    protected AbstractServer(StateFactory<E, ER, Q, QR> stateFactory, Serializer<E> entrySerializer,
                             Serializer<ER> entryResultSerializer, Serializer<Q> querySerializer,
                             Serializer<QR> resultSerializer,
                             JournalEntryParser journalEntryParser, ScheduledExecutorService scheduledExecutor,
                             ExecutorService asyncExecutor, ServerRpcAccessPoint serverRpcAccessPoint, Properties properties){
        this.journalEntryParser = journalEntryParser;
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.config = toConfig(properties);
        this.threads.createThread(buildStateMachineThread());
        this.threads.createThread(buildFlushJournalThread());
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.serverRpcAccessPoint = serverRpcAccessPoint;
        this.properties = properties;
        this.stateFactory = stateFactory;
        this.voterConfigManager = new VoterConfigManager(journalEntryParser);
        // init metrics
        if(config.isEnableMetric()) {
            this.metricFactory = ServiceSupport.load(JMetricFactory.class);
            this.metricMap = new ConcurrentHashMap<>();
            if(config.getPrintMetricIntervalSec() > 0) {
                this.threads.createThread(buildPrintMetricThread());
            }
        } else {
            this.metricFactory = null;
            this.metricMap = null;
        }
        applyEntriesMetric = getMetric(METRIC_APPLY_ENTRIES);


        this.eventBus = new EventBus(config.getRpcTimeoutMs());
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();
        bufferPool = ServiceSupport.load(BufferPool.class);
        journal = new Journal(
                persistenceFactory,
                bufferPool, journalEntryParser);
        this.state = new JournalKeeperState<>(stateFactory, metadataPersistence);

        state.addInterceptor(InternalEntryType.TYPE_SCALE_PARTITIONS, this::scalePartitions);
        state.addInterceptor(InternalEntryType.TYPE_LEADER_ANNOUNCEMENT, this::announceLeader);
        state.addInterceptor(InternalEntryType.TYPE_CREATE_SNAPSHOT, this::createSnapShot);
    }

    private AsyncLoopThread buildStateMachineThread() {
        return ThreadBuilder.builder()
                .name(STATE_MACHINE_THREAD)
                .doWork(this::applyEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("{} Exception: ", STATE_MACHINE_THREAD, e))
                .daemon(true)
                .build();
    }


    private AsyncLoopThread buildFlushJournalThread() {
        return ThreadBuilder.builder()
                .name(FLUSH_JOURNAL_THREAD)
                .doWork(this::flushJournal)
                .sleepTime(config.getFlushIntervalMs(), config.getFlushIntervalMs())
                .onException(e -> logger.warn("{} Exception: ", FLUSH_JOURNAL_THREAD, e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildPrintMetricThread() {
        return ThreadBuilder.builder()
                .name(PRINT_METRIC_THREAD)
                .doWork(this::printMetrics)
                .sleepTime(config.getPrintMetricIntervalSec() * 1000, config.getPrintMetricIntervalSec() * 1000)
                .onException(e -> logger.warn("{} Exception: ", PRINT_METRIC_THREAD, e))
                .daemon(true)
                .build();
    }

    @Override
    public synchronized void init(URI uri, List<URI> voters, Set<Integer> userPartitions, URI preferredLeader) throws IOException {
        ReservedPartition.validatePartitions(userPartitions);
        this.uri = uri;
        Set<Integer> partitions = new HashSet<>(userPartitions);
        partitions.add(INTERNAL_PARTITION);
        state.init(statePath(),voters, partitions, preferredLeader);
        createFistSnapshot(voters, partitions, preferredLeader);
        lastSavedServerMetadata = createServerMetadata();
        metadataPersistence.save(metadataFile(),lastSavedServerMetadata);
    }

    private void createFistSnapshot(List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        JournalKeeperState<E, ER, Q, QR> snapshot = new JournalKeeperState<>(stateFactory, metadataPersistence);
        snapshot.init(snapshotsPath().resolve(String.valueOf(0L)), voters, partitions, preferredLeader);
    }

    @Override
    public boolean isInitialized() {
        try {
            ServerMetadata  metadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
            return metadata != null && metadata.isInitialized();
        } catch (Exception e) {
            return false;
        }
    }

    int getTerm(long index) {
        try {
            return journal.getTerm(index);
        } catch (IndexUnderflowException e) {
            if(index  + 1 == snapshots.firstKey()) {
                return snapshots.firstEntry().getValue().lastIncludedTerm();
            } else {
                throw e;
            }
        }
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

        config.setEnableMetric(Boolean.parseBoolean(
                properties.getProperty(
                        Config.ENABLE_METRIC_KEY,
                        String.valueOf(Config.DEFAULT_ENABLE_METRIC))));

        config.setPrintMetricIntervalSec(Integer.parseInt(
                properties.getProperty(
                        Config.PRINT_METRIC_INTERVAL_SEC_KEY,
                        String.valueOf(Config.DEFAULT_PRINT_METRIC_INTERVAL_SEC))));

        return config;
    }

    protected Path workingDir() {
        return config.getWorkingDir();
    }

    protected Path snapshotsPath() {
        return workingDir().resolve(SNAPSHOTS_PATH);
    }

    protected Path statePath() {
        return workingDir().resolve(STATE_PATH);
    }
    protected Path metadataPath() {
        return workingDir().resolve(METADATA_PATH);
    }
    protected Path metadataFile() {
        return workingDir().resolve(METADATA_PATH).resolve(METADATA_FILE);
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
        while (this.serverState == ServerState.RUNNING && state.lastApplied() < journal.commitIndex()) {
            applyEntriesMetric.start();

            JournalEntry journalEntry = journal.read(state.lastApplied());
            StateResult<ER> stateResult = state.applyEntry(journalEntry, entrySerializer, journal);
            afterStateChanged(stateResult.getUserResult());

            Map<String, String> parameters = new HashMap<>(stateResult.getEventData().size() + 1);
            stateResult.getEventData().forEach(parameters::put);
            parameters.put("lastApplied", String.valueOf(state.lastApplied()));
            fireEvent(EventType.ON_STATE_CHANGE, parameters);
            applyEntriesMetric.end(journalEntry.getLength());
        }
    }

    private void fireOnLeaderChangeEvent(int term) {
        Map<String, String> eventData = new HashMap<>();
        eventData.put("leader", String.valueOf(this.leaderUri));
        eventData.put("term", String.valueOf(term));
        fireEvent(EventType.ON_LEADER_CHANGE, eventData);
    }

    private void announceLeader(InternalEntryType type, byte [] internalEntry) {
        LeaderAnnouncementEntry leaderAnnouncementEntry = InternalEntriesSerializeSupport.parse(internalEntry);
        fireOnLeaderChangeEvent(leaderAnnouncementEntry.getTerm());
    }

    private void scalePartitions(InternalEntryType type, byte [] internalEntry) {
        ScalePartitionsEntry scalePartitionsEntry = InternalEntriesSerializeSupport.parse(internalEntry);
        Set<Integer> partitions = scalePartitionsEntry.getPartitions();
        try {
            Set<Integer> currentPartitions = journal.getPartitions();
            currentPartitions.removeIf(p -> p >= RESERVED_PARTITIONS_START);

            for (int partition : partitions) {
                if (!currentPartitions.contains(partition)) {
                    journal.addPartition(partition);
                }
            }

            List<Integer> toBeRemoved = new ArrayList<>();
            for (Integer partition: currentPartitions) {
                if (!partitions.contains(partition)) {
                    toBeRemoved.add(partition);
                }
            }
            for (Integer partition : toBeRemoved) {
                journal.removePartition(partition);
            }
            logger.info("Journal repartitioned, partitions: {}, path: {}.",
                    journal.getPartitions(), journalPath().toAbsolutePath().toString());
        } catch (IOException e) {
            throw new JournalException(e);
        }
    }

    protected void fireEvent(int eventType, Map<String, String> eventData) {
        eventBus.fireEvent(new Event(eventType, eventData));
    }

    /**
     * 当状态变化后触发事件
     * @param updateResult 状态机执行结果
     */
    protected void afterStateChanged(ER updateResult) {}
    /**
     * 如果需要，保存一次快照
     */

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StateQueryResult<QR> queryResult = state.query(querySerializer.parse(request.getQuery()), journal);
                return new QueryStateResponse(resultSerializer.serialize(queryResult.getResult()), queryResult.getLastApplied());
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

                if (request.getIndex() == state.lastApplied()) {
                    StateQueryResult<QR> queryResult = state.query(querySerializer.parse(request.getQuery()), journal);
                    if(queryResult.getLastApplied() == request.getIndex()) {
                        return new QueryStateResponse(resultSerializer.serialize(queryResult.getResult()), queryResult.getLastApplied());
                    }
                }

                JournalKeeperState<E, ER, Q, QR> snapshot;
                Map.Entry<Long, JournalKeeperState<E, ER, Q, QR>> nearestSnapshot = snapshots.floorEntry(request.getIndex());
                if (null == nearestSnapshot) {
                    throw new IndexUnderflowException();
                }

                if(request.getIndex() == nearestSnapshot.getKey()) {
                    snapshot = nearestSnapshot.getValue();
                } else {
                    snapshot = new JournalKeeperState<>(stateFactory, metadataPersistence);
                    Path tempSnapshotPath = snapshotsPath().resolve(String.valueOf(request.getIndex()));
                    if (Files.exists(tempSnapshotPath)) {
                        throw new ConcurrentModificationException(String.format("A snapshot of position %d is creating, please retry later.", request.getIndex()));
                    }
                    nearestSnapshot.getValue().dump(tempSnapshotPath);
                    snapshot.recover(tempSnapshotPath, properties);

                    while (snapshot.lastApplied() < request.getIndex()) {
                        snapshot.applyEntry(journal.read(snapshot.lastApplied()), entrySerializer, journal);
                    }
                    snapshot.flush();

                    snapshots.putIfAbsent(request.getIndex(), snapshot);
                }
                return new QueryStateResponse(resultSerializer.serialize(snapshot.query(querySerializer.parse(request.getQuery()), journal).getResult()));
            } catch (Throwable throwable) {
                return new QueryStateResponse(throwable);
            }
        }, asyncExecutor);
    }

    private void createSnapShot(InternalEntryType type, byte [] internalEntry) {
        if (type == InternalEntryType.TYPE_CREATE_SNAPSHOT) {
            createSnapshot();
        }
    }
    private void createSnapshot() {
        createSnapshot(state.lastApplied());
    }
    private void createSnapshot(long lastApplied) {
        Path snapshotPath = snapshotsPath().resolve(String.valueOf(lastApplied));
        try {
            state.dump(snapshotPath);
            JournalKeeperState<E, ER, Q, QR> snapshot = new JournalKeeperState<>(stateFactory, metadataPersistence);
            snapshot.recover(snapshotPath, properties);
            snapshot.createJournalSnapshot(journal);

            snapshots.put(snapshot.lastApplied(), snapshot);
        } catch (IOException e) {
            logger.warn("Create snapshot exception! Snapshot: {}.", snapshotPath, e);
        }
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
        return CompletableFuture.supplyAsync(() ->
                new GetServersResponse(
                        new ClusterConfiguration(leaderUri, state.voters(), observers)),
                asyncExecutor);
    }

    // TODO
    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
//            if(!snapshots.isEmpty()) {
//
//                long snapshotIndex = request.getLastIncludedIndex() + 1;
//                if (snapshotIndex < 0) {
//                    snapshotIndex = snapshots.lastKey();
//                }
//                JournalKeeperState<E, ER, Q, QR> state = snapshots.get(snapshotIndex);
//                if (null != state) {
//                    byte [] data;
//                    try {
//                        data = state.readSerializedTrunk(request.getOffset(),config.getGetStateBatchSize());
//                    } catch (IOException e) {
//                        throw new CompletionException(e);
//                    }
//                    return new GetServerStateResponse(
//                            state.lastIncludedIndex(), state.lastIncludedTerm(),
//                            request.getOffset(),
//                            data,
//                            request.getOffset() + data.length >= state.serializedDataSize());
//                }
//            }
            return new GetServerStateResponse(new NoSuchSnapshotException());
        }, asyncExecutor).exceptionally(GetServerStateResponse::new);
    }

    @Override
    public final void start() {
        if(this.serverState != ServerState.CREATED) {
            throw new IllegalStateException("AbstractServer can only start once!");
        }
        this.serverState = ServerState.STARTING;
        doStart();
        threads.startThread(STATE_MACHINE_THREAD);
        threads.startThread(FLUSH_JOURNAL_THREAD);
        if(threads.exists(PRINT_METRIC_THREAD)) {
            threads.startThread(PRINT_METRIC_THREAD);
        }
        flushStateFuture = scheduledExecutor.scheduleAtFixedRate(this::flushState,
                ThreadLocalRandom.current().nextLong(10L, 50L),
                config.getFlushIntervalMs(), TimeUnit.MILLISECONDS);
        this.serverState = ServerState.RUNNING;
    }

    protected abstract void doStart();
    /**
     * 刷盘：
     * 1. 日志
     * 2. 状态
     * 3. 元数据
     */
    private void flushAll() {
        journal.flush();
        flushState();
    }

    private void flushJournal() {
        this.journal.flush();
        onJournalFlushed();
    }

    protected void onJournalFlushed() {}

    private void flushState() {
        try {
            if (state instanceof Flushable) {
                ((Flushable) state).flush();
            }
            ServerMetadata metadata = createServerMetadata();
            if (!metadata.equals(lastSavedServerMetadata)) {
                metadataPersistence.save(metadataFile(), metadata);
                lastSavedServerMetadata = metadata;
            }
        } catch(Throwable e) {
            logger.warn("Flush exception, commitIndex: {}, lastApplied: {}, server: {}: ",
                    journal.commitIndex(), state.lastApplied(), uri, e);
        }
    }

    public CompletableFuture<ServerRpc> getServerRpc(URI uri) {
        return CompletableFuture.completedFuture(
                remoteServers.computeIfAbsent(uri, uri1 -> serverRpcAccessPoint.getServerRpcAgent(uri1)));
    }

    @Override
    public final void stop() {
        try {
            this.serverState = ServerState.STOPPING;
            doStop();
            remoteServers.values().forEach(ServerRpc::stop);
            waitJournalApplied();
            threads.stopThread(STATE_MACHINE_THREAD);
            threads.stopThread(FLUSH_JOURNAL_THREAD);
            if(threads.exists(PRINT_METRIC_THREAD)) {
                threads.stopThread(PRINT_METRIC_THREAD);
            }

            stopAndWaitScheduledFeature(flushStateFuture, 1000L);
            if(persistenceFactory instanceof Closeable) {
                ((Closeable) persistenceFactory).close();
            }
            flushAll();
            this.serverState = ServerState.STOPPED;
        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception: ", t);
        }
    }

    private void waitJournalApplied() throws InterruptedException {
        while (journal.commitIndex() < state.lastApplied()) {
            Thread.sleep(50L);
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
    public synchronized void recover() throws IOException {
        lastSavedServerMetadata = metadataPersistence.load(metadataFile(), ServerMetadata.class);
        if(lastSavedServerMetadata == null || !lastSavedServerMetadata.isInitialized()) {
            throw new RecoverException(
                    String.format("Recover failed! Cause: metadata is not initialized. Metadata path: %s.",
                            metadataFile().toString()));
        }
        onMetadataRecovered(lastSavedServerMetadata);
        state.recover(statePath(), properties);
        recoverSnapshots();
        recoverJournal(state.getPartitions(), snapshots.firstEntry().getValue().getJournalSnapshot(), lastSavedServerMetadata.getCommitIndex());
        onJournalRecovered(journal);
    }

    protected void onJournalRecovered(Journal journal) {
        recoverVoterConfig();
    }



    /**
     * Check reserved entries to ensure the last UpdateVotersConfig entry is applied to the current voter config.
     */
    private void recoverVoterConfig() {
        boolean isRecoveredFromJournal = false;
        for(long index = journal.maxIndex(INTERNAL_PARTITION) - 1;
            index >= journal.minIndex(INTERNAL_PARTITION);
            index --) {
            JournalEntry entry = journal.readByPartition(INTERNAL_PARTITION, index);
            InternalEntryType type = InternalEntriesSerializeSupport.parseEntryType(entry.getPayload().getBytes());

            if(type == InternalEntryType.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                state.setConfigState(new ConfigState(
                        updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew()));
                isRecoveredFromJournal = true;
                break;
            } else if(type == InternalEntryType.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = InternalEntriesSerializeSupport.parse(entry.getPayload().getBytes());
                state.setConfigState(new ConfigState(updateVotersS2Entry.getConfigNew()));
                isRecoveredFromJournal = true;
                break;
            }
        }

        if (isRecoveredFromJournal) {
            logger.info("Voters config is recovered from journal.");
        } else {
            logger.info("No voters config entry found in journal, Using config in the metadata.");
        }
        logger.info(state.getConfigState().toString());
    }


    private void recoverJournal(Set<Integer> partitions, JournalSnapshot journalSnapshot, long commitIndex) throws IOException {
        journal.recover(journalPath(), commitIndex, journalSnapshot,  properties);
        journal.rePartition(partitions);
    }

    private Path journalPath() {
        return workingDir();
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
                    JournalKeeperState<E, ER, Q, QR> snapshot = new JournalKeeperState<>(stateFactory, metadataPersistence);
                    snapshot.recover(path, properties);
                    if(Long.parseLong(path.getFileName().toString()) == snapshot.lastApplied()) {
                        return snapshot;
                    } else {
                        snapshot.close();
                        return null;
                    }
                }).filter(Objects::nonNull)
                .forEach(snapshot -> snapshots.put(snapshot.lastApplied(), snapshot));
    }

    protected void onMetadataRecovered(ServerMetadata metadata) {
//        if(lastSavedServerMetadata.isJointConsensus()) {
//            votersConfigStateMachine = new ConfigState(
//                    lastSavedServerMetadata.getOldVoters(),
//                    lastSavedServerMetadata.getVoters()
//            );
//        } else {
//            votersConfigStateMachine =
//                    new ConfigState(lastSavedServerMetadata.getVoters());
//        }
        this.uri = metadata.getThisServer();

//        if(metadata.getPartitions() == null ) {
//            metadata.setPartitions(new HashSet<>());
//        }

//        if(metadata.getPartitions().isEmpty()) {
//            metadata.getPartitions().addAll(
//                    Stream.of(RaftJournal.DEFAULT_PARTITION, INTERNAL_PARTITION)
//                            .collect(Collectors.toSet())
//            );
//        }

    }



    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = new ServerMetadata();
        serverMetadata.setInitialized(true);
        serverMetadata.setThisServer(uri);
//        ConfigState config = votersConfigStateMachine.clone();
//        serverMetadata.setPartitions(journal.getPartitions());
//        serverMetadata.setVoters(config.getConfigNew());
//        serverMetadata.setOldVoters(config.getConfigOld());
//        serverMetadata.setJointConsensus(config.isJointConsensus());
        serverMetadata.setCommitIndex(journal.commitIndex());
        return serverMetadata;
    }

    protected long randomInterval(long interval) {
        return interval + Math.round(ThreadLocalRandom.current().nextDouble(-1 * RAND_INTERVAL_RANGE, RAND_INTERVAL_RANGE) * interval);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return CompletableFuture.supplyAsync(() ->
                new GetServerEntriesResponse(
                        journal.readRaw(request.getIndex(), (int) Math.min(request.getMaxSize(), state.lastApplied() - request.getIndex())),
                        journal.minIndex(), state.lastApplied()), asyncExecutor)
                .exceptionally(e -> {
                    try {
                        throw e;
                    } catch (CompletionException ce) {
                        return new GetServerEntriesResponse(ce.getCause());
                    } catch (Throwable throwable) {
                        return new GetServerEntriesResponse(throwable);
                    }
                });
    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ServerState serverState() {
        return this.serverState;
    }

    protected long getRpcTimeoutMs() {
        return config.getRpcTimeoutMs();
    }

    public void compact(long indexExclusive) throws IOException {
            Map.Entry<Long, JournalKeeperState<E, ER, Q, QR>> nearestEntry = snapshots.floorEntry(indexExclusive);
            if(null != nearestEntry) {
                SortedMap<Long, JournalKeeperState<E, ER, Q, QR>> toBeRemoved = snapshots.headMap(nearestEntry.getKey(), false);
                while (!toBeRemoved.isEmpty()) {
                    toBeRemoved.remove(toBeRemoved.firstKey()).clear();
                }

                journal.compact(nearestEntry.getValue().getJournalSnapshot());
            }
    }

    public JournalKeeperState<E, ER, Q, QR> getState() {
        return state;
    }

    /**
     * This method returns the metric instance of given name, instance will be created if not exists.
     * if config.isEnableMetric() equals false, just return a dummy metric.
     * @param name name of the metric
     * @return the metric instance of given name.
     */
    @Override
    public JMetric getMetric(String name) {
        if(config.isEnableMetric()) {
            return metricMap.computeIfAbsent(name, metricFactory::create);
        } else {
            return DUMMY_METRIC;
        }
    }
    @Override
    public boolean isMetricEnabled() {return config.isEnableMetric();}
    @Override
    public void removeMetric(String name) {
        if(config.isEnableMetric()) {
            metricMap.remove(name);
        }
    }

    private void printMetrics() {
        metricMap.values()
            .stream()
                .map(JMetric::getAndReset)
                .map(JMetricSupport::formatNs)
                .forEach(logger::info);
        onPrintMetric();
    }


    // for monitor only

    URI getLeaderUri() {
        return leaderUri;
    }

    Journal getJournal() {
        return journal;
    }


    /**
     * This method will be invoked when metric
     */
    protected void onPrintMetric() {}
    public static class Config {
        public final static int DEFAULT_SNAPSHOT_STEP = 0;
        public final static long DEFAULT_RPC_TIMEOUT_MS = 1000L;
        public final static long DEFAULT_FLUSH_INTERVAL_MS = 50L;
        public final static int DEFAULT_GET_STATE_BATCH_SIZE = 1024 * 1024;
        public final static boolean DEFAULT_ENABLE_METRIC = false;
        public final static int DEFAULT_PRINT_METRIC_INTERVAL_SEC = 0;

        public final static String SNAPSHOT_STEP_KEY = "snapshot_step";
        public final static String RPC_TIMEOUT_MS_KEY = "rpc_timeout_ms";
        public final static String FLUSH_INTERVAL_MS_KEY = "flush_interval_ms";
        public final static String WORKING_DIR_KEY = "working_dir";
        public final static String GET_STATE_BATCH_SIZE_KEY = "get_state_batch_size";
        public final static String ENABLE_METRIC_KEY = "enable_metric";
        public final static String PRINT_METRIC_INTERVAL_SEC_KEY = "print_metric_interval_sec";

        private int snapshotStep = DEFAULT_SNAPSHOT_STEP;
        private long rpcTimeoutMs = DEFAULT_RPC_TIMEOUT_MS;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private Path workingDir = Paths.get(System.getProperty("user.dir")).resolve("journalkeeper");
        private int getStateBatchSize = DEFAULT_GET_STATE_BATCH_SIZE;
        private boolean enableMetric = DEFAULT_ENABLE_METRIC;
        private int printMetricIntervalSec = DEFAULT_PRINT_METRIC_INTERVAL_SEC;
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

        public boolean isEnableMetric() {
            return enableMetric;
        }

        public void setEnableMetric(boolean enableMetric) {
            this.enableMetric = enableMetric;
        }

        public int getPrintMetricIntervalSec() {
            return printMetricIntervalSec;
        }

        public void setPrintMetricIntervalSec(int printMetricIntervalSec) {
            this.printMetricIntervalSec = printMetricIntervalSec;
        }
    }
}
