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

import io.journalkeeper.base.ReplicableIterator;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.entry.internal.CreateSnapshotEntry;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.LeaderAnnouncementEntry;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.state.ApplyInternalEntryInterceptor;
import io.journalkeeper.core.state.ApplyReservedEntryInterceptor;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.JournalKeeperState;
import io.journalkeeper.core.state.Snapshot;
import io.journalkeeper.core.transaction.JournalTransactionManager;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.utils.async.Async;
import io.journalkeeper.utils.state.ServerStateMachine;
import io.journalkeeper.utils.state.StateServer;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import io.journalkeeper.utils.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;
import static io.journalkeeper.core.server.MetricNames.METRIC_APPEND_ENTRIES_RPC;
import static io.journalkeeper.core.server.ThreadNames.FLUSH_JOURNAL_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_APPEND_ENTRY_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_CALLBACK_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_COMMIT_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_REPLICATION_THREAD;
import static io.journalkeeper.core.server.ThreadNames.STATE_MACHINE_THREAD;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
class Leader extends ServerStateMachine implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class);
    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected final JournalKeeperState state;
    /**
     * 客户端更新状态请求队列
     */
    private final BlockingQueue<UpdateStateRequestResponse> pendingUpdateStateRequests;
    /**
     * 保存异步响应的回调方法
     */
    private final CallbackResultBelt replicationCallbacks;
    private final CallbackResultBelt flushCallbacks;
    /**
     * 当角色为LEADER时，记录所有FOLLOWER的位置等信息
     */
    private final List<ReplicationDestination> followers = new CopyOnWriteArrayList<>();
    /**
     * 刷盘位置，用于回调
     */
    private final AtomicLong journalFlushIndex = new AtomicLong(0L);
    private final Threads threads;
    private final long heartbeatIntervalMs;
    private final int replicationBatchSize;
    private final long rpcTimeoutMs;
    private final Journal journal;
    /**
     * 存放节点上所有状态快照的稀疏数组，数组的索引（key）就是快照对应的日志位置的索引
     */
    private final Map<Long, Snapshot> immutableSnapshots;

    private final URI serverUri;
    private final int currentTerm;
    private final Map<URI, JMetric> appendEntriesRpcMetricMap;
    private final ServerRpcProvider serverRpcProvider;
    private final ScheduledExecutorService scheduledExecutor;
    private final VoterConfigManager voterConfigManager;
    private final MetricProvider metricProvider;
    private final AtomicBoolean writeEnabled = new AtomicBoolean(true);
    private final JournalEntryParser journalEntryParser;
    private final JournalTransactionManager journalTransactionManager;
    private final ApplyReservedEntryInterceptor journalTransactionInterceptor;
    private final ApplyInternalEntryInterceptor leaderAnnouncementInterceptor;
    private final NavigableMap<Long, Snapshot> snapshots;
    private final int snapshotIntervalSec;
    private final AtomicBoolean isLeaderAnnouncementApplied = new AtomicBoolean(false);
    private final AtomicLong callbackBarrier = new AtomicLong(0L);
    /**
     * Leader有效期，用于读取状态时判断leader是否还有效，每次从Follower收到心跳响应，定时更新leader的有效期。
     */
    private AtomicLong leaderShipDeadLineMs = new AtomicLong(0L);
    private JMetric updateClusterStateMetric;
    private JMetric appendJournalMetric;
    private ScheduledFuture takeSnapshotFuture;
    private AtomicBoolean isAnyFollowerNextIndexUpdated = new AtomicBoolean(false);

    Leader(Journal journal, JournalKeeperState state, Map<Long, Snapshot> immutableSnapshots,
           int currentTerm,
           URI serverUri,
           int cacheRequests, long heartbeatIntervalMs, long rpcTimeoutMs, int replicationBatchSize,
           int snapshotIntervalSec,
           Threads threads,
           ServerRpcProvider serverRpcProvider,
           ClientServerRpc server,
           ScheduledExecutorService scheduledExecutor,
           VoterConfigManager voterConfigManager,
           MetricProvider metricProvider,
           JournalEntryParser journalEntryParser,
           long transactionTimeoutMs, NavigableMap<Long, Snapshot> snapshots) {

        super(true);
        this.pendingUpdateStateRequests = new LinkedBlockingQueue<>(cacheRequests);
        this.state = state;
        this.serverUri = serverUri;
        this.replicationBatchSize = replicationBatchSize;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.currentTerm = currentTerm;
        this.immutableSnapshots = immutableSnapshots;
        this.snapshotIntervalSec = snapshotIntervalSec;
        this.threads = threads;
        this.serverRpcProvider = serverRpcProvider;
        this.scheduledExecutor = scheduledExecutor;
        this.voterConfigManager = voterConfigManager;
        this.metricProvider = metricProvider;
        this.journalEntryParser = journalEntryParser;
        this.snapshots = snapshots;
        this.replicationCallbacks = new RingBufferBelt(rpcTimeoutMs, cacheRequests);
        this.flushCallbacks = new RingBufferBelt(rpcTimeoutMs, cacheRequests);
        this.appendEntriesRpcMetricMap = new HashMap<>(2);
        this.journal = journal;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.journalTransactionManager = new JournalTransactionManager(journal, server, scheduledExecutor, transactionTimeoutMs);
        this.journalTransactionInterceptor = (entryHeader, entryFuture, index) -> journalTransactionManager.applyEntry(entryHeader, entryFuture);
        this.leaderAnnouncementInterceptor = (type, internalEntry) -> {
            if (type == InternalEntryType.TYPE_LEADER_ANNOUNCEMENT) {
                LeaderAnnouncementEntry leaderAnnouncementEntry = InternalEntriesSerializeSupport.parse(internalEntry);
                if (leaderAnnouncementEntry.getTerm() == currentTerm) {
                    logger.info("Leader announcement applied! Leader: {}, term: {}.", serverUri, currentTerm);
                    isLeaderAnnouncementApplied.compareAndSet(false, true);
                }
            }
        };

        this.callbackBarrier.set(journal.maxIndex());
    }

    private AsyncLoopThread buildLeaderAppendJournalEntryThread() {
        return ThreadBuilder.builder()
                .name(threadName(LEADER_APPEND_ENTRY_THREAD))
                .doWork(this::appendJournalEntry)
                .sleepTime(0, 0)
                .onException(new DefaultExceptionListener(LEADER_APPEND_ENTRY_THREAD))
                .daemon(true)
                .build();
    }


    private AsyncLoopThread buildLeaderReplicationResponseThread() {
        return ThreadBuilder.builder()
                .name(threadName(LEADER_COMMIT_THREAD))
                .doWork(this::commit)
                .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                .onException(new DefaultExceptionListener(LEADER_COMMIT_THREAD))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildCallbackThread() {
        return ThreadBuilder.builder()
                .name(threadName(LEADER_CALLBACK_THREAD))
                .doWork(this::callback)
                .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                .onException(new DefaultExceptionListener(LEADER_CALLBACK_THREAD))
                .daemon(true)
                .build();
    }

    private String threadName(String staticThreadName) {
        return serverUri + "-" + staticThreadName;
    }

    /**
     * 串行写入日志
     */
    private void appendJournalEntry() throws Exception {

        UpdateStateRequestResponse rr = pendingUpdateStateRequests.take();
        final UpdateClusterStateRequest request = rr.getRequest();
        final ResponseFuture responseFuture = rr.getResponseFuture();
        try {

            if (request.getRequests().size() == 1 && voterConfigManager.maybeUpdateLeaderConfig(request.getRequests().get(0),
                    state.getConfigState(), journal, () -> doAppendJournalEntryCallable(request, responseFuture),
                    serverUri, this)) {
                return;
            }
            doAppendJournalEntry(request, responseFuture);

        } catch (Throwable t) {
            responseFuture.getResponseFuture().complete(new UpdateClusterStateResponse(t));
            throw t;
        }

    }

    private Void doAppendJournalEntryCallable(UpdateClusterStateRequest request, ResponseFuture responseFuture) throws InterruptedException {
        doAppendJournalEntry(request, responseFuture);
        return null;
    }

    private void doAppendJournalEntry(UpdateClusterStateRequest request, ResponseFuture responseFuture) throws InterruptedException {
        appendJournalMetric.start();

        List<JournalEntry> journalEntries = new ArrayList<>(request.getRequests().size());
        for (UpdateRequest serializedUpdateRequest : request.getRequests()) {
            JournalEntry entry;

            if (request.isIncludeHeader()) {
                entry = journalEntryParser.parse(serializedUpdateRequest.getEntry());
            } else {
                entry = journalEntryParser.createJournalEntry(serializedUpdateRequest.getEntry());
            }
            entry.setPartition(serializedUpdateRequest.getPartition());
            entry.setBatchSize(serializedUpdateRequest.getBatchSize());
            entry.setTerm(currentTerm);


            if (request.getTransactionId() != null) {
                entry = journalTransactionManager.wrapTransactionalEntry(entry, request.getTransactionId(), journalEntryParser);
            }
            journalEntries.add(entry);
        }
        appendAndCallback(journalEntries, request.getResponseConfig(), responseFuture);
        wakeupReplicationThreads();
        threads.wakeupThread(threadName(FLUSH_JOURNAL_THREAD));
        appendJournalMetric.end(() -> journalEntries.stream().mapToLong(JournalEntry::getLength).sum());
    }

    private void wakeupReplicationThreads() {
        for (ReplicationDestination follower : followers) {
            try {
                threads.wakeupThread(follower.getReplicationThreadName());
            } catch (NoSuchElementException e) {
                logger.warn("Wake up {} failed, follower {} maybe removed.", follower.getReplicationThreadName(), follower.getUri());
            }
        }
        if (followers.isEmpty()) {
            threads.wakeupThread(threadName(LEADER_COMMIT_THREAD));
        }
    }

    private void appendAndCallback(List<JournalEntry> journalEntries, ResponseConfig responseConfig, ResponseFuture responseFuture) throws InterruptedException {
        if (journalEntries.size() == 1) {
            long offset = journal.append(journalEntries.get(0));
            setCallback(responseConfig, responseFuture, offset);
        } else {
            List<Long> offsets = journal.append(journalEntries);
            for (Long offset : offsets) {
                setCallback(responseConfig, responseFuture, offset);
            }
        }
    }

    private void setCallback(ResponseConfig responseConfig, ResponseFuture responseFuture, long offset) throws InterruptedException {
        if (responseConfig == ResponseConfig.REPLICATION) {
            replicationCallbacks.put(new Callback(offset, responseFuture));
        } else if (responseConfig == ResponseConfig.PERSISTENCE) {
            flushCallbacks.put(new Callback(offset, responseFuture));
        } else if (responseConfig == ResponseConfig.ALL) {
            replicationCallbacks.put(new Callback(offset, responseFuture));
            flushCallbacks.put(new Callback(offset, responseFuture));
        }
        callbackBarrier.set(offset);
    }

    private int getPreLogTerm(long currentLogIndex) {
        if (currentLogIndex > journal.minIndex()) {
            return journal.getTerm(currentLogIndex - 1);
        } else if (currentLogIndex == journal.minIndex() && immutableSnapshots.containsKey(currentLogIndex)) {
            return immutableSnapshots.get(currentLogIndex).lastIncludedTerm();
        } else if (currentLogIndex == 0) {
            return -1;
        } else {
            throw new IndexUnderflowException();
        }
    }

    /**
     * 对于每一个AsyncAppendRequest RPC请求，当收到成功响应的时需要更新repStartIndex、matchIndex和commitIndex。
     * 由于接收者按照日志的索引位置串行处理请求，一般情况下，收到的响应也是按照顺序返回的，但是考虑到网络延时和数据重传，
     * 依然不可避免乱序响应的情况。LEADER在处理响应时需要遵循：
     * <p>
     * 1. 对于所有响应，先比较返回值中的term是否与当前term一致，如果不一致说明任期已经变更，丢弃响应，
     * 2. LEADER 反复重试所有term一致的超时和失败请求（考虑到性能问题，可以在每次重试前加一个时延）；
     * 3. 对于返回失败的请求，如果这个请求是所有在途请求中日志位置最小的（repStartIndex == logIndex），
     * 说明接收者的日志落后于repStartIndex，这时LEADER需要回退，再次发送AsyncAppendRequest RPC请求，
     * 直到找到FOLLOWER与LEADER相同的位置。
     * 4. 对于成功的响应，需要按照日志索引位置顺序处理。规定只有返回值中的logIndex与repStartIndex相等时，
     * 才更新repStartIndex和matchIndex，否则反复重试直到满足条件；
     * 5. 如果存在一个索引位置N，这个N是所有满足如下所有条件位置中的最大值，则将commitIndex更新为N。
     * 5.1 超过半数的matchIndex都大于等于N
     * 5.2 N > commitIndex
     * 5.3 log[N].term == currentTerm
     */
    private void commit() throws IOException {
        while (serverState() == ServerState.RUNNING &&
                !Thread.currentThread().isInterrupted() &&
                (journal.commitIndex()< journal.maxIndex() && (
                        followers.isEmpty() || isAnyFollowerNextIndexUpdated.get()
                        ))) {
            ConfigState configState = state.getConfigState();
            List<ReplicationDestination> finalFollowers = new ArrayList<>(followers);
            long N = 0L;
            if (finalFollowers.isEmpty()) {
                N = journal.maxIndex();
            } else {

                if (isAnyFollowerNextIndexUpdated.compareAndSet(true, false)) {
                    if (configState.isJointConsensus()) {
                        long[] sortedMatchIndexInOldConfig = finalFollowers.stream()
                                .filter(follower -> configState.getConfigOld().contains(follower.getUri()))
                                .mapToLong(ReplicationDestination::getMatchIndex)
                                .sorted().toArray();
                        long nInOldConfig = sortedMatchIndexInOldConfig.length > 0 ?
                                sortedMatchIndexInOldConfig[sortedMatchIndexInOldConfig.length / 2] : journal.maxIndex();

                        long[] sortedMatchIndexInNewConfig = finalFollowers.stream()
                                .filter(follower -> configState.getConfigNew().contains(follower.getUri()))
                                .mapToLong(ReplicationDestination::getMatchIndex)
                                .sorted().toArray();
                        long nInNewConfig = sortedMatchIndexInNewConfig.length > 0 ?
                                sortedMatchIndexInNewConfig[sortedMatchIndexInNewConfig.length / 2] : journal.maxIndex();

                        N = Math.min(nInNewConfig, nInOldConfig);

                    } else {
                        long[] sortedMatchIndex = finalFollowers.stream()
                                .mapToLong(ReplicationDestination::getMatchIndex)
                                .sorted().toArray();
                        if (sortedMatchIndex.length > 0) {
                            N = sortedMatchIndex[sortedMatchIndex.length / 2];
                        }
                    }

                }
            }
            if (N > journal.commitIndex() && getTerm(N - 1) == currentTerm) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Set commitIndex {} to {}, {}.", journal.commitIndex(), N, voterInfo());
                }
                journal.commit(N);
                onCommitted();
            }
        }
    }

    private void onCommitted() {
        // 唤醒状态机线程
        threads.wakeupThread(threadName(STATE_MACHINE_THREAD));
        wakeupReplicationThreads();
    }

    private int getTerm(long index) {
        try {
            return journal.getTerm(index);
        } catch (IndexUnderflowException e) {
            if (index + 1 == snapshots.firstKey()) {
                return snapshots.firstEntry().getValue().lastIncludedTerm();
            } else {
                throw e;
            }
        }
    }

    private void installSnapshot(ReplicationDestination follower, Snapshot snapshot) {

        try {
            logger.info("Install snapshot to {} ...", follower.getUri());
            ServerRpc rpc = serverRpcProvider.getServerRpc(follower.getUri()).get(heartbeatIntervalMs, TimeUnit.MILLISECONDS);
            int offset = 0;
            ReplicableIterator iterator = snapshot.iterator();
            while (iterator.hasMoreTrunks()) {
                byte[] trunk = iterator.nextTrunk();
                InstallSnapshotRequest request = new InstallSnapshotRequest(
                        currentTerm, serverUri, snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                        offset, trunk, !iterator.hasMoreTrunks()
                );
                InstallSnapshotResponse response = rpc.installSnapshot(request).get();
                if (!response.success()) {
                    logger.warn("Install snapshot to {} failed! Cause: {}.", follower.getUri(), response.errorString());
                    return;
                }
                offset += trunk.length;
            }
            logger.info("Install snapshot to {} success!", follower.getUri());
        } catch (Throwable t) {
            logger.warn("Install snapshot to {} failed!", follower.getUri(), t);
        }
    }

    private void callback() {
        long callbackIndex = journalFlushIndex.get();
        if (callbackIndex > callbackBarrier.get()) {
            flushCallbacks.callbackBefore(callbackBarrier.get());
        }
        while (callbackIndex > callbackBarrier.get()) {
            Thread.yield();
        }
        flushCallbacks.callbackBefore(callbackIndex);
    }

    private String voterInfo() {
        return String.format("voterState: %s, currentTerm: %d, minIndex: %d, " +
                        "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                VoterState.LEADER, currentTerm, journal.minIndex(),
                journal.maxIndex(), journal.commitIndex(), state.lastApplied(), serverUri.toString());
    }

    CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {

        UpdateStateRequestResponse requestResponse = new UpdateStateRequestResponse(request, updateClusterStateMetric);

        try {
            if (serverState() != ServerState.RUNNING) {
                throw new IllegalStateException(String.format("Leader is not RUNNING, state: %s.", serverState().toString()));
            }

            if (!writeEnabled.get()) {
                throw new IllegalStateException("Server disabled temporarily.");
            }

            pendingUpdateStateRequests.put(requestResponse);
            if (request.getResponseConfig() == ResponseConfig.RECEIVE) {
                requestResponse.getResponseFuture().getResponseFuture().complete(new UpdateClusterStateResponse());
            }
        } catch (Throwable e) {
            requestResponse.getResponseFuture().getResponseFuture().complete(new UpdateClusterStateResponse(e));
        }
        return requestResponse.getResponseFuture().getResponseFuture();
    }

    void disableWrite(long timeoutMs, int term) {
        if (currentTerm != term) {
            throw new IllegalStateException(
                    String.format("Term not matched! Term in leader: %d, term in request: %d", currentTerm, term));
        }
        writeEnabled.set(false);
        scheduledExecutor.schedule(() -> writeEnabled.set(true), timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStart() {
        super.doStart();
        // 初始化followers
        this.followers.addAll(state.getConfigState().voters().stream()
                .filter(uri -> !uri.equals(serverUri))
                .map(uri -> new ReplicationDestination(uri, journal.maxIndex()))
                .collect(Collectors.toList()));

        this.followers.forEach(follower ->
                appendEntriesRpcMetricMap.put(
                        follower.getUri(),
                        metricProvider.getMetric(MetricNames.compose(METRIC_APPEND_ENTRIES_RPC, follower.getUri()))));
        this.appendJournalMetric = metricProvider.getMetric(MetricNames.METRIC_APPEND_JOURNAL);
        this.updateClusterStateMetric = metricProvider.getMetric(MetricNames.METRIC_UPDATE_CLUSTER_STATE);


        this.threads.createThread(buildLeaderAppendJournalEntryThread());
        this.threads.createThread(buildLeaderReplicationResponseThread());
        this.threads.createThread(buildCallbackThread());
        this.threads.startThread(threadName(LEADER_COMMIT_THREAD));
        this.followers.forEach(ReplicationDestination::start);
        this.threads.startThread(threadName(LEADER_CALLBACK_THREAD));
        this.threads.startThread(threadName(LEADER_APPEND_ENTRY_THREAD));

        journalTransactionManager.start();
        state.addInterceptor(this.journalTransactionInterceptor);
        state.addInterceptor(InternalEntryType.TYPE_LEADER_ANNOUNCEMENT, this.leaderAnnouncementInterceptor);
        if (snapshotIntervalSec > 0) {
            takeSnapshotFuture = scheduledExecutor.scheduleAtFixedRate(this::takeSnapshotPeriodically,
                    ThreadLocalRandom.current().nextLong(0, snapshotIntervalSec),
                    snapshotIntervalSec, TimeUnit.SECONDS);
        }
        appendLeaderAnnouncementEntry();
    }

    private void appendLeaderAnnouncementEntry() {
        // Leader announcement
        try {
            byte[] payload = InternalEntriesSerializeSupport.serialize(new LeaderAnnouncementEntry(currentTerm, serverUri));
            JournalEntry journalEntry = journalEntryParser.createJournalEntry(payload);
            journalEntry.setTerm(currentTerm);
            journalEntry.setPartition(INTERNAL_PARTITION);
            appendAndCallback(Collections.singletonList(journalEntry), null, null);
        } catch (InterruptedException e) {
            logger.warn("Exception: ", e);
        }
    }

    private void takeSnapshotPeriodically() {
        if (state.lastApplied() > snapshots.lastKey()) {
            logger.info("Send create snapshot request.");
            updateClusterState(
                    new UpdateClusterStateRequest(
                            new UpdateRequest(InternalEntriesSerializeSupport.serialize(
                                    new CreateSnapshotEntry()), RaftJournal.INTERNAL_PARTITION, 1
                            )
                    )
            );
        } else {
            logger.info("No entry since last snapshot, no need to create a new snapshot.");
        }

    }

    @Override
    protected void doStop() {
        mayBeWaitingForAppendJournals();

        if (takeSnapshotFuture != null) {
            takeSnapshotFuture.cancel(true);
        }

        state.removeInterceptor(InternalEntryType.TYPE_LEADER_ANNOUNCEMENT, leaderAnnouncementInterceptor);
        state.removeInterceptor(this.journalTransactionInterceptor);
        journalTransactionManager.stop();
        this.threads.stopThread(threadName(LEADER_APPEND_ENTRY_THREAD));
        this.followers.forEach(ReplicationDestination::stop);

        this.threads.stopThread(threadName(LEADER_CALLBACK_THREAD));
        failAllPendingCallbacks();
        this.threads.stopThread(threadName(LEADER_COMMIT_THREAD));
        this.threads.removeThread(threadName(LEADER_APPEND_ENTRY_THREAD));
        this.threads.removeThread(threadName(LEADER_CALLBACK_THREAD));
        this.threads.removeThread(threadName(LEADER_COMMIT_THREAD));
        removeAppendEntriesRpcMetrics();
        metricProvider.removeMetric(MetricNames.METRIC_APPEND_JOURNAL);
        metricProvider.removeMetric(MetricNames.METRIC_UPDATE_CLUSTER_STATE);
        super.doStop();

    }

    private void mayBeWaitingForAppendJournals() {
        while (!pendingUpdateStateRequests.isEmpty()) {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 给所有没来及处理的请求返回失败响应
    private void failAllPendingCallbacks() {

        replicationCallbacks.failAll();
        flushCallbacks.failAll();
    }

    private void removeAppendEntriesRpcMetrics() {
        appendEntriesRpcMetricMap.forEach((followerUri, metric) ->
                metricProvider.removeMetric(MetricNames.compose(METRIC_APPEND_ENTRIES_RPC, followerUri)));
    }

    void callback(long lastApplied, byte[] result) {
        while (lastApplied > callbackBarrier.get()) {
            Thread.yield();
        }
        replicationCallbacks.callback(lastApplied, result);
    }

    void onJournalFlushed() {

        journalFlushIndex.set(journal.maxIndex());
        if (serverState() == ServerState.RUNNING) {
            threads.wakeupThread(threadName(LEADER_APPEND_ENTRY_THREAD));
            threads.wakeupThread(threadName(LEADER_CALLBACK_THREAD));
        }

    }

    /**
     * 异步检测Leader有效性，成功返回null，失败抛出异常。
     */
    CompletableFuture<Void> waitLeadership() {
        return waitLeadership(System.currentTimeMillis() + rpcTimeoutMs);
    }

    private CompletableFuture<Void> waitLeadership(long deadLineTimestamp) {
        if (System.currentTimeMillis() > deadLineTimestamp) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new NotLeaderException(null));
            return future;
        } else if (checkLeadership()) {
            return CompletableFuture.completedFuture(null);
        } else {
            return Async.scheduleAsync(scheduledExecutor, () -> waitLeadership(deadLineTimestamp), heartbeatIntervalMs / 10, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * LEADER有效性检查
     * 只读的操作可以直接处理而不需要记录日志。
     * 但是，在不增加任何限制的情况下，这么做可能会冒着返回过期数据的风险，因为LEADER响应客户端请求时可能已经被新的LEADER废除了，
     * 但是它还不知道。LEADER在处理只读的请求之前必须检查自己是否已经被废除了。RAFT中通过让领导人在响应只读请求之前，
     * 先和集群中的大多数节点交换一次心跳信息来处理这个问题。考虑到和每次只读都进行一轮心跳交换时延较高，
     * JournalKeeper采用一种近似的有效性检查。LEADER记录每个FOLLOWER最近返回的心跳成功响应时间戳，每次处理只读请求之前，检查这些时间戳，
     * 如果半数以上的时间戳距离当前时间的差值不大于平均心跳间隔，则认为LEADER当前有效，
     * 否则反复重新检查（这段时间有可能会有新的心跳响应回来更新上次心跳时间），直到成功或者超时。
     */
    private boolean checkLeadership() {
        if (!isLeaderAnnouncementApplied.get()) {
            return false;
        }

        if (followers.isEmpty()) {
            return true;
        }

        if (System.currentTimeMillis() <= leaderShipDeadLineMs.get()) {
            return true;
        } else {
            // 重新计算leaderShipDeadLineMs

            long[] sortedHeartbeatResponseTimes = followers.stream().mapToLong(ReplicationDestination::getLastHeartbeatResponseTime)
                    .sorted().toArray();

            leaderShipDeadLineMs.set(
                    (sortedHeartbeatResponseTimes[sortedHeartbeatResponseTimes.length / 2]) + heartbeatIntervalMs);
            return System.currentTimeMillis() <= leaderShipDeadLineMs.get();
        }
    }

    CompletableFuture<JournalKeeperTransactionContext> createTransaction(Map<String, String> context) {
        return journalTransactionManager.createTransaction(context);
    }

    CompletableFuture<Void> completeTransaction(UUID transactionId, boolean commitOrAbort) {
        return journalTransactionManager.completeTransaction(transactionId, commitOrAbort);
    }

    Collection<JournalKeeperTransactionContext> getOpeningTransactions() {
        return journalTransactionManager.getOpeningTransactions();
    }

    // for monitor only
    int getRequestQueueSize() {
        return pendingUpdateStateRequests.size();
    }

    boolean isWriteEnabled() {
        return writeEnabled.get();
    }

    List<Leader.ReplicationDestination> getFollowers() {
        return Collections.unmodifiableList(followers);
    }

    void addFollower(URI followerUri) {
        ReplicationDestination follower = new Leader.ReplicationDestination(followerUri, journal.maxIndex());
        follower.start();
        followers.add(follower);
        logger.info("Leader add follower {}",follower);
    }

    void removeFollower(URI followerUri) {
        ReplicationDestination follower = followers.stream().filter(f -> Objects.equals(f.getUri(), followerUri)).findAny().orElse(null);
        if(null != follower) {
            follower.stop();
            appendEntriesRpcMetricMap.remove(followerUri);
            followers.remove(follower);
            logger.info("Leader remove follower {}",follower);
        }
    }

    Collection<URI> getFollowerUris() {
        return followers.stream().map(ReplicationDestination::getUri).collect(Collectors.toSet());
    }

    private static class UpdateStateRequestResponse {
        private final UpdateClusterStateRequest request;
        private final ResponseFuture responseFuture;
        private final long start = System.nanoTime();

        UpdateStateRequestResponse(UpdateClusterStateRequest request, JMetric metric) {
            this.request = request;
            this.responseFuture = new ResponseFuture(request.getResponseConfig(), request.getRequests().size());
            if (null != metric) {
                responseFuture.getResponseFuture()
                        .thenRun(() -> metric.mark(() -> System.nanoTime() - start, () -> request.getRequests().stream().mapToLong(r -> r.getEntry().length).sum()));
            }

        }

        UpdateClusterStateRequest getRequest() {
            return request;
        }

        ResponseFuture getResponseFuture() {
            return this.responseFuture;
        }
    }


    class ReplicationDestination {


        private final URI uri;

        /**
         * 需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
         */
        private long nextIndex;
        /**
         * 已经复制到该服务器的日志的最高索引值（从 0 开始递增）
         */
        private long matchIndex = 0L;

        /**
         * 上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
         */
        private long lastHeartbeatResponseTime;
        private long lastHeartbeatRequestTime = 0L;

        private final String replicationThreadName;
        private final JMetric metric;


        ReplicationDestination(URI uri, long nextIndex) {
            this.uri = uri;
            this.nextIndex = nextIndex;
            this.lastHeartbeatResponseTime = 0L;
            replicationThreadName = LEADER_REPLICATION_THREAD + "-" + serverUri +  "->" + uri;
            metric = Leader.this.appendEntriesRpcMetricMap.get(uri);
        }

        private AsyncLoopThread buildLeaderReplicationThread() {
            return ThreadBuilder.builder()
                    .name(replicationThreadName)
                    .doWork(this::replication)
                    .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                    .onException(new DefaultExceptionListener(replicationThreadName))
                    .daemon(true)
                    .build();
        }

        String getReplicationThreadName() {
            return replicationThreadName;
        }

        private void start() {
            Leader.this.threads.createThread(buildLeaderReplicationThread());
            Leader.this.threads.startThread(replicationThreadName);
        }

        private void stop() {
            Leader.this.threads.stopThread(replicationThreadName);
            Leader.this.threads.removeThread(replicationThreadName);
        }

        private void replication() {
            long maxIndex;
            while (serverState() == ServerState.RUNNING &&
                    !Thread.currentThread().isInterrupted() &&
                    (nextIndex < (maxIndex = journal.maxIndex()) // 还有需要复制的数据
                    ||
                    System.currentTimeMillis() - lastHeartbeatRequestTime >= heartbeatIntervalMs) // 距离上次复制/心跳已经超过一个心跳超时了
            ) {
                long start = metric == null ? 0L : System.nanoTime();

                // 如果有必要，先安装第一个快照
                Map.Entry<Long, Snapshot> fistSnapShotEntry = snapshots.firstEntry();
                maybeInstallSnapshotFirst(fistSnapShotEntry);

                // 读取需要复制的Entry
                List<byte[]> entries;
                if (nextIndex < maxIndex) { // 复制
                    entries = journal.readRaw(nextIndex, Leader.this.replicationBatchSize);
                } else { // 心跳
                    entries = Collections.emptyList();
                }

                // 构建请求并发送
                AsyncAppendEntriesRequest request =
                        new AsyncAppendEntriesRequest(Leader.this.currentTerm, Leader.this.serverUri,
                                nextIndex - 1, Leader.this.getPreLogTerm(nextIndex),
                                entries, journal.commitIndex(), maxIndex);
                AsyncAppendEntriesResponse response = null;
                try {
                    response = serverRpcProvider.getServerRpc(uri)
                            .thenCompose(serverRpc -> serverRpc.asyncAppendEntries(request)).get();
                    lastHeartbeatRequestTime = System.currentTimeMillis();
                } catch (InterruptedException ie) {
                    logger.warn("Replication was interrupted, from {} to {}.", Leader.this.serverUri, uri);
                    Thread.currentThread().interrupt();
                    break;
                } catch (ExecutionException e) {
                    logger.warn("Replication execution exception, from {} to {}, cause: {}.", Leader.this.serverUri, uri, null == e.getCause()? e.getMessage() : e.getCause().getMessage());
                } catch (Throwable t) {
                    logger.warn("Replication exception, from {} to {}, cause: {}.", Leader.this.serverUri, uri, t.getMessage());
                }

                // 处理返回的响应
                if(null != response && response.success()) { // 成功收到响应响应
                    lastHeartbeatResponseTime = System.currentTimeMillis();

                    if (response.isSuccess()) { // 复制成功
                        if (entries.size() > 0) {
                            nextIndex += entries.size();
                            matchIndex = nextIndex;
                            isAnyFollowerNextIndexUpdated.compareAndSet(false, true);
                            Leader.this.threads.wakeupThread(Leader.this.threadName(LEADER_COMMIT_THREAD));
                        }
                    } else {
                        // 不匹配，回退
                        int rollbackSize = (int) Math.min(replicationBatchSize, nextIndex - fistSnapShotEntry.getKey());
                        nextIndex -= rollbackSize;
                    }
                    if (null != metric) {
                        metric.mark(() -> System.nanoTime() - start, () -> request.getEntries().stream().mapToLong(e -> e.length).sum());
                    }
                } else { // 没收到响应或者请求失败
                    // 等下一个心跳超时之后，再进入这个方法会自动重试
                    break;
                }

            }
        }

        private void maybeInstallSnapshotFirst(Map.Entry<Long, Snapshot> fistSnapShotEntry) {
            if (nextIndex <= fistSnapShotEntry.getKey()) {
                installSnapshot(this, fistSnapShotEntry.getValue());
                nextIndex = fistSnapShotEntry.getKey();
            }
        }

        URI getUri() {
            return uri;
        }

        long getNextIndex() {
            return nextIndex;
        }

        long getMatchIndex() {
            return matchIndex;
        }

        long getLastHeartbeatResponseTime() {
            return lastHeartbeatResponseTime;
        }

        long getLastHeartbeatRequestTime() {
            return lastHeartbeatRequestTime;
        }

        @Override
        public String toString() {
            return "{" +
                    "uri=" + uri +
                    ", nextIndex=" + nextIndex +
                    ", matchIndex=" + matchIndex +
                    '}';
        }
    }


}
