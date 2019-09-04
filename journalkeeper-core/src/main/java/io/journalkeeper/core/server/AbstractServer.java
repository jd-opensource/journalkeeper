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

import com.sun.istack.internal.NotNull;
import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.entry.Entry;
import io.journalkeeper.core.entry.EntryHeader;
import io.journalkeeper.core.entry.JournalEntryParser;
import io.journalkeeper.core.entry.reserved.CompactJournalEntry;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedEntry;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS1Entry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS2Entry;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.metric.DummyMetric;
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
import io.journalkeeper.rpc.RpcAccessPointFactory;
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
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
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
import io.journalkeeper.utils.state.StateServer;
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.Callable;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITION;

// TODO: Add an UUID for each server instance,
//  so multiple server instance of one process
//  can be identified by UUID in logs and threads dump.

/**
 * Server就是集群中的节点，它包含了存储在Server上日志（journal），一组快照（snapshots[]）和一个状态机（stateMachine）实例。
 * @author LiYue
 * Date: 2019-03-14
 */
public abstract class AbstractServer<E, ER, Q, QR>
        extends RaftServer<E, ER, Q, QR>
        implements ServerRpc {
    private static final Logger logger = LoggerFactory.getLogger(AbstractServer.class);
    /**
     * 每个Server模块中需要运行一个用于执行日志更新状态，保存Snapshot的状态机线程，
     */
    protected final static String STATE_MACHINE_THREAD = "StateMachineThread";
    /**
     * 刷盘Journal线程
     */
    protected final static String FLUSH_JOURNAL_THREAD = "FlushJournalThread";
    /**
     * 打印Metric线程
     */
    protected final static String PRINT_METRIC_THREAD = "PrintMetricThread";

    /**
     * 当前集群配置
     */
    protected VoterConfigurationStateMachine votersConfigStateMachine;

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
    protected final State<E, ER, Q, QR> state;

    protected final ScheduledExecutorService scheduledExecutor;

    protected final ExecutorService asyncExecutor;
    /**
     * 心跳间隔、选举超时等随机时间的随机范围
     */
    public final static float RAND_INTERVAL_RANGE = 0.25F;

//    /**
//     * 所有选民节点地址，包含LEADER
//     */
//    protected List<URI> voters;

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
    protected final NavigableMap<Long, State<E, ER, Q, QR>> snapshots = new ConcurrentSkipListMap<>();

    /**
     * 当前LEADER节点地址
     */
    protected URI leader;

    /**
     * 观察者节点
     */
    protected List<URI> observers;



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
    protected final Serializer<ER> entryResultSerializer;
    protected final Serializer<Q> querySerializer;
    protected final Serializer<QR> resultSerializer;

    protected final BufferPool bufferPool;

    protected ServerRpcAccessPoint serverRpcAccessPoint;
    protected final RpcAccessPointFactory rpcAccessPointFactory;
    protected StateServer rpcServer = null;

    protected final Map<URI, ServerRpc> remoteServers = new HashMap<>();
    private Config config;

    private volatile ServerState serverState = ServerState.CREATED;
    /**
     * 上次保存的元数据
     */
    private ServerMetadata lastSavedServerMetadata = null;

    protected final EventBus eventBus;

    protected final Lock scalePartitionLock = new ReentrantLock(true);
    protected final Lock compactLock = new ReentrantLock(true);

    protected final Threads threads = ThreadsFactory.create();

    private final JMetricFactory metricFactory;
    private final Map<String, JMetric> metricMap;
    private final static JMetric DUMMY_METRIC = new DummyMetric();

    private final static String METRIC_EXEC_STATE_MACHINE = "EXEC_STATE_MACHINE";
    private final static String METRIC_APPLY_ENTRIES = "APPLY_ENTRIES";

    private final JMetric execStateMachineMetric;
    private final JMetric applyEntriesMetric;

    public AbstractServer(StateFactory<E, ER, Q, QR> stateFactory, Serializer<E> entrySerializer,
                          Serializer<ER> entryResultSerializer, Serializer<Q> querySerializer,
                          Serializer<QR> resultSerializer, ScheduledExecutorService scheduledExecutor,
                          ExecutorService asyncExecutor, Properties properties){
        super(stateFactory, properties);
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.config = toConfig(properties);
        this.threads.createThread(buildStateMachineThread());
        this.threads.createThread(buildFlushJournalThread());
        this.state = stateFactory.createState();
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.entryResultSerializer = entryResultSerializer;

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
        execStateMachineMetric = getMetric(METRIC_EXEC_STATE_MACHINE);
        applyEntriesMetric = getMetric(METRIC_APPLY_ENTRIES);


        this.eventBus = new EventBus(config.getRpcTimeoutMs());
        persistenceFactory = ServiceSupport.load(PersistenceFactory.class);
        metadataPersistence = persistenceFactory.createMetadataPersistenceInstance();
        bufferPool = ServiceSupport.load(BufferPool.class);
        rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);
        journal = new Journal(
                persistenceFactory,
                bufferPool);

    }

    private AsyncLoopThread buildStateMachineThread() {
        return ThreadBuilder.builder()
                .name(STATE_MACHINE_THREAD)
                .condition(() ->this.serverState() == ServerState.RUNNING)
                .doWork(this::applyEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("{} Exception: ", STATE_MACHINE_THREAD, e))
                .daemon(true)
                .build();
    }


    private AsyncLoopThread buildFlushJournalThread() {
        return ThreadBuilder.builder()
                .name(FLUSH_JOURNAL_THREAD)
                .condition(() ->this.serverState() == ServerState.RUNNING)
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
    public synchronized void init(URI uri, List<URI> voters) throws IOException {

        this.uri = uri;
        votersConfigStateMachine = new VoterConfigurationStateMachine(voters);
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
        while ( this.serverState == ServerState.RUNNING && state.lastApplied() < journal.commitIndex()) {
            applyEntriesMetric.start();

            takeASnapShotIfNeed();
            Entry storageEntry = journal.read(state.lastApplied());
            Map<String, String> customizedEventData = new HashMap<>();
            ER result = null;
            if(storageEntry.getHeader().getPartition() != RESERVED_PARTITION) {
                E entry = entrySerializer.parse(storageEntry.getEntry());
                long stamp = stateLock.writeLock();
                try {
                    execStateMachineMetric.start();
                    result = state.execute(entry, storageEntry.getHeader().getPartition(),
                            state.lastApplied(), storageEntry.getHeader().getBatchSize(), customizedEventData);
                    execStateMachineMetric.end(storageEntry.getEntry().length);
                } finally {
                    stateLock.unlockWrite(stamp);
                }
            } else {
                applyReservedEntry(storageEntry.getEntry());
            }


            beforeStateChanged(result);
            state.next();
            afterStateChanged(result);

            Map<String, String> parameters = new HashMap<>(customizedEventData.size() + 1);
            customizedEventData.forEach(parameters::put);
            parameters.put("lastApplied", String.valueOf(state.lastApplied()));
            fireEvent(EventType.ON_STATE_CHANGE, parameters);
            applyEntriesMetric.end(storageEntry.getEntry().length);
        }
    }

    protected void applyReservedEntry(byte [] reservedEntry) {
        int type = ReservedEntriesSerializeSupport.parseEntryType(reservedEntry);
        logger.info("Apply reserved entry, type: {}", type);
        switch (type) {
            case ReservedEntry
                    .TYPE_LEADER_ANNOUNCEMENT:
                // Nothing to do.
                break;
            case ReservedEntry.TYPE_COMPACT_JOURNAL:
                compactJournalAsync(ReservedEntriesSerializeSupport.parse(reservedEntry, CompactJournalEntry.class).getCompactIndices());
                break;
            case ReservedEntry.TYPE_SCALE_PARTITIONS:
                scalePartitions(ReservedEntriesSerializeSupport.parse(reservedEntry, ScalePartitionsEntry.class).getPartitions());
                break;
            case ReservedEntry.TYPE_UPDATE_VOTERS_S1:
                byte [] s2Entry = ReservedEntriesSerializeSupport.serialize(new UpdateVotersS2Entry(votersConfigStateMachine.getConfigOld(), votersConfigStateMachine.getConfigNew()));
                    try {
                        if(votersConfigStateMachine.isJointConsensus()) {
                            updateClusterState(new UpdateClusterStateRequest(s2Entry, RESERVED_PARTITION, 1, ResponseConfig.ONE_WAY)).get();
                        } else {
                            throw new IllegalStateException();
                        }
                    } catch (Exception e) {
                        UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(reservedEntry);
                        logger.warn("Failed to update voter config in step 2! Config in the first step entry from: {} To: {}, " +
                                        "voter config old: {}, new: {}.",
                                updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew(),
                                votersConfigStateMachine.getConfigOld(), votersConfigStateMachine.getConfigNew());
                    }
                break;
            case ReservedEntry.TYPE_UPDATE_VOTERS_S2:
                // Stop myself if I'm no longer a member of the cluster.
                if(roll() == Roll.VOTER && !votersConfigStateMachine.voters().contains(this.serverUri())) {
                    this.stopAsync();
                }
                break;
            default:
                logger.warn("Invalid reserved entry type: {}.", type);
        }
    }

    protected void maybeUpdateConfigOnReplication(List<byte []> entries) throws Exception {
        for (byte[] rawEntry : entries) {
            ByteBuffer entryBuffer = ByteBuffer.wrap(rawEntry);
            EntryHeader entryHeader = JournalEntryParser.parseHeader(entryBuffer);
            if(entryHeader.getPartition() == RESERVED_PARTITION) {
                byte [] payload = JournalEntryParser.getEntry(rawEntry);
                int entryType = ReservedEntriesSerializeSupport.parseEntryType(payload);
                if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                    UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(payload);

                    votersConfigStateMachine.toJointConsensus(updateVotersS1Entry.getConfigNew(),
                            () -> null);
                } else if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                    votersConfigStateMachine.toNewConfig(() -> null);
                    // Stop myself if I'm no longer a member of the cluster.
                    if(roll() == Roll.VOTER && !votersConfigStateMachine.voters().contains(this.serverUri())) {
                        this.stopAsync();
                    }
                }
            }
        }
    }

    private void scalePartitions(int[] partitions) {
        try {
            scalePartitionLock.lock();
            journal.rePartition(Arrays.stream(partitions).boxed().collect(Collectors.toSet()));
        } finally {
            scalePartitionLock.unlock();
        }

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
     * 当状态变化后触发事件
     */
    protected void afterStateChanged(ER updateResult) {}
    /**
     * 当状态变化前触发事件
     */
    protected void beforeStateChanged(ER updateResult) {}

    /**
     * 如果需要，保存一次快照
     */
    private void takeASnapShotIfNeed() {
        if(config.getSnapshotStep() > 0) {
            asyncExecutor.submit(() -> {
                try {
                    synchronized (snapshots) {
                        if (config.getSnapshotStep() > 0 && (snapshots.isEmpty() || state.lastApplied() - snapshots.lastKey() > config.getSnapshotStep())) {

                            State<E, ER, Q, QR> snapshot = state.takeASnapshot(snapshotsPath().resolve(String.valueOf(state.lastApplied())), journal);
                            snapshots.put(snapshot.lastApplied(), snapshot);
                        }
                    }
                } catch (IOException e) {
                    logger.warn("Take snapshot exception: ", e);
                }
            });
        }
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


                State<E, ER, Q, QR> requestState = snapshots.get(request.getIndex());

                if (null == requestState) {
                    Map.Entry<Long, State<E, ER, Q, QR>> nearestSnapshot = snapshots.floorEntry(request.getIndex());
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
                        requestState.execute(entrySerializer.parse(entry.getEntry()), entry.getHeader().getPartition(),
                                nearestSnapshot.getKey() + i, entry.getHeader().getBatchSize(), new HashMap<>());
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
        return CompletableFuture.supplyAsync(() ->
                new GetServersResponse(
                        new ClusterConfiguration(leader, votersConfigStateMachine.voters(), observers)),
                asyncExecutor);
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            if(!snapshots.isEmpty()) {

                long snapshotIndex = request.getLastIncludedIndex() + 1;
                if (snapshotIndex < 0) {
                    snapshotIndex = snapshots.lastKey();
                }
                State<E, ER, Q, QR> state = snapshots.get(snapshotIndex);
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
        if(this.serverState != ServerState.CREATED) {
            throw new IllegalStateException("AbstractServer can only start once!");
        }
        this.serverState = ServerState.STARTING;
        doStart();
        threads.start();
        flushFuture = scheduledExecutor.scheduleAtFixedRate(this::flushState,
                ThreadLocalRandom.current().nextLong(10L, 50L),
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
                metadataPersistence.save(metadata);
                lastSavedServerMetadata = metadata;
            }
        } catch(Throwable e) {
            logger.warn("Flush exception, commitIndex: {}, lastApplied: {}, server: {}: ",
                    journal.commitIndex(), state.lastApplied(), uri, e);
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
            threads.stop();
            stopAndWaitScheduledFeature(flushFuture, 1000L);
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
        lastSavedServerMetadata = metadataPersistence.recover(metadataPath(), properties);
        onMetadataRecovered(lastSavedServerMetadata);
        recoverJournal(lastSavedServerMetadata.getPartitions(), lastSavedServerMetadata.getCommitIndex());
        onJournalRecovered(journal);
        state.recover(statePath(), journal, properties);
        recoverSnapshots();
    }

    protected void onJournalRecovered(Journal journal) {
        recoverVoterConfig();
    }



    /**
     * Check reserved entries to ensure the last UpdateVotersConfig entry is applied to the current voter config.
     */
    private void recoverVoterConfig() {
        boolean isRecoveredFromJournal = false;
        for(long index = journal.maxIndex(RESERVED_PARTITION) - 1;
            index >= journal.minIndex(RESERVED_PARTITION);
            index --) {
            RaftEntry entry = journal.readByPartition(RESERVED_PARTITION, index);
            int type = ReservedEntriesSerializeSupport.parseEntryType(entry.getEntry());

            if(type == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(entry.getEntry());
                votersConfigStateMachine = new VoterConfigurationStateMachine(
                        updateVotersS1Entry.getConfigOld(), updateVotersS1Entry.getConfigNew());
                isRecoveredFromJournal = true;
                break;
            } else if(type == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = ReservedEntriesSerializeSupport.parse(entry.getEntry());
                votersConfigStateMachine = new VoterConfigurationStateMachine(updateVotersS2Entry.getConfigNew());
                isRecoveredFromJournal = true;
                break;
            }
        }

        if (isRecoveredFromJournal) {
            logger.info("Voters config is recovered from journal.");
        } else {
            logger.info("No voters config entry found in journal, Using config in the metadata.");
        }
        logger.info(votersConfigStateMachine.toString());
    }


    private void recoverJournal(Set<Integer> partitions, long commitIndex) throws IOException {
        journal.recover(journalPath(), commitIndex, properties);
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
                    State<E, ER, Q, QR> snapshot = stateFactory.createState();
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
        if(lastSavedServerMetadata.isJointConsensus()) {
            votersConfigStateMachine = new VoterConfigurationStateMachine(
                    lastSavedServerMetadata.getOldVoters(),
                    lastSavedServerMetadata.getVoters()
            );
        } else {
            votersConfigStateMachine =
                    new VoterConfigurationStateMachine(lastSavedServerMetadata.getVoters());
        }
        this.uri = metadata.getThisServer();

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
        VoterConfigurationStateMachine config = votersConfigStateMachine.clone();
        serverMetadata.setVoters(config.configNew);
        serverMetadata.setOldVoters(config.configOld);
        serverMetadata.setJointConsensus(config.jointConsensus);
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
        if(config.getSnapshotStep() > 0) {
            Map.Entry<Long, State<E, ER, Q, QR>> nearestEntry = snapshots.floorEntry(indexExclusive);
            SortedMap<Long, State<E, ER, Q, QR>> toBeRemoved = snapshots.headMap(nearestEntry.getKey());
            while (!toBeRemoved.isEmpty()) {
                toBeRemoved.remove(toBeRemoved.firstKey()).clear();
            }
            journal.compact(nearestEntry.getKey());
        } else {
            journal.compact(indexExclusive);
        }
    }

    public State<E, ER, Q, QR> getState() {
        return state;
    }

    /**
     * This method returns the metric instance of given name, instance will be created if not exists.
     * if config.isEnableMetric() equals false, just return a dummy metric.
     * @param name name of the metric
     * @return the metric instance of given name.
     */
    @NotNull
    protected JMetric getMetric(String name) {
        if(config.isEnableMetric()) {
            return metricMap.computeIfAbsent(name, metricFactory::create);
        } else {
            return DUMMY_METRIC;
        }
    }
    protected boolean isMetricEnabled() {return config.isEnableMetric();}
    protected void removeMetric(String name) {
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

    /**
     * Server当前集群配置的状态机，线程安全。包括二个状态：
     * 普通状态：常态。
     * 共同一致状态：变更进群配置过程中的中间状态。
     */
    protected static class VoterConfigurationStateMachine {
        private final List<URI> configNew = new ArrayList<>(3);
        private final List<URI> configOld = new ArrayList<>(3);
        private boolean jointConsensus;
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

        // all voters include configNew and configOld
        private List<URI> allVoters = new ArrayList<>(3);

        private VoterConfigurationStateMachine(List<URI> configOld, List<URI> configNew) {
            jointConsensus = true;
            this.configOld.addAll(configOld);
            this.configNew.addAll(configNew);

            buildAllVoters();
        }

        private void buildAllVoters() {
            allVoters = Stream.concat(configNew.stream(), configOld.stream())
                    .distinct()
                    .collect(Collectors.toList());
        }

        private VoterConfigurationStateMachine(List<URI> configNew) {
            jointConsensus = false;
            this.configNew.addAll(configNew);
            buildAllVoters();
        }

        protected List<URI> getConfigNew() {
            rwLock.readLock().lock();
            try {
                return new ArrayList<>(configNew);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        protected List<URI> getConfigOld() {
            rwLock.readLock().lock();
            try {
                return new ArrayList<>(configOld);
            } finally {
                rwLock.readLock().unlock();
            }
        }

        protected boolean isJointConsensus() {
            rwLock.readLock().lock();
            try {
                return jointConsensus;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        protected List<URI> voters() {
            rwLock.readLock().lock();
            try {
                return allVoters;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        protected void toNewConfig(Callable appendEntryCallable) throws Exception {
            rwLock.writeLock().lock();
            try {
                if(!jointConsensus) {
                    throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == true, actual: false.");
                }
                appendEntryCallable.call();
                jointConsensus = false;
                configOld.clear();
                buildAllVoters();

            }finally {
                rwLock.writeLock().unlock();
            }
        }

        protected void toJointConsensus(List<URI> configNew, Callable appendEntryCallable) throws Exception {
            rwLock.writeLock().lock();
            try {
                if(jointConsensus) {
                    throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == false, actual: true.");
                }

                appendEntryCallable.call();
                jointConsensus = true;
                this.configOld.addAll(this.configNew);
                this.configNew.clear();
                this.configNew.addAll(configNew);
                buildAllVoters();

            }finally {
                rwLock.writeLock().unlock();
            }
        }

        @Override
        protected VoterConfigurationStateMachine clone() {
            rwLock.readLock().lock();
            try {
                if(jointConsensus) {
                    return new VoterConfigurationStateMachine(new ArrayList<>(configOld), new ArrayList<>(configNew));
                } else {
                    return new VoterConfigurationStateMachine(new ArrayList<>(configNew));
                }

            } finally {
                rwLock.readLock().unlock();
            }
        }

        @Override
        public String toString() {
            rwLock.readLock().lock();
            try {
                String str = "Voters config: jointConsensus: " +
                        jointConsensus + ", ";
                if(jointConsensus) {
                    str += "old config: [" +
                            configOld.stream().map(URI::toString).collect(Collectors.joining(", ")) + "], ";
                    str += "new config: [" +
                            configNew.stream().map(URI::toString).collect(Collectors.joining(", ")) + "].";
                } else {
                    str += "config: [" +
                            configNew.stream().map(URI::toString).collect(Collectors.joining(", ")) + "].";
                }
                return str;
            } finally {
                rwLock.readLock().unlock();
            }
        }

        public void rollbackToOldConfig() {
            rwLock.writeLock().lock();
            try {
                if(!jointConsensus) {
                    throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == true, actual: false.");
                }
                jointConsensus = false;
                configNew.clear();
                configNew.addAll(configOld);
                configOld.clear();
                buildAllVoters();

            }finally {
                rwLock.writeLock().unlock();
            }
        }

        public void rollbackToJointConsensus(List<URI> configOld) {
            rwLock.writeLock().lock();
            try {
                if(jointConsensus) {
                    throw new IllegalStateException("Invalid joint consensus state! expected: jointConsensus == false, actual: true.");
                }

                jointConsensus = true;
                this.configOld.addAll(configOld);
                buildAllVoters();

            }finally {
                rwLock.writeLock().unlock();
            }
        }
    }


    /**
     * This method will be invoked when metric
     */
    protected void onPrintMetric() {}
    static class Config {
        final static int DEFAULT_SNAPSHOT_STEP = 0;
        final static long DEFAULT_RPC_TIMEOUT_MS = 1000L;
        final static long DEFAULT_FLUSH_INTERVAL_MS = 50L;
        final static int DEFAULT_GET_STATE_BATCH_SIZE = 1024 * 1024;
        final static boolean DEFAULT_ENABLE_METRIC = false;
        final static int DEFAULT_PRINT_METRIC_INTERVAL_SEC = 0;

        final static String SNAPSHOT_STEP_KEY = "snapshot_step";
        final static String RPC_TIMEOUT_MS_KEY = "rpc_timeout_ms";
        final static String FLUSH_INTERVAL_MS_KEY = "flush_interval_ms";
        final static String WORKING_DIR_KEY = "working_dir";
        final static String GET_STATE_BATCH_SIZE_KEY = "get_state_batch_size";
        final static String ENABLE_METRIC_KEY = "enable_metric";
        final static String PRINT_METRIC_INTERVAL_SEC_KEY = "print_metric_interval_sec";

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
