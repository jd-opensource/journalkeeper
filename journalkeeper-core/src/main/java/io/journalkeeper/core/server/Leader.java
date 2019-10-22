package io.journalkeeper.core.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.journalkeeper.core.server.MetricNames.METRIC_APPEND_ENTRIES_RPC;
import static io.journalkeeper.core.server.ThreadNames.FLUSH_JOURNAL_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_APPEND_ENTRY_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_CALLBACK_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_REPLICATION_RESPONSES_HANDLER_THREAD;
import static io.journalkeeper.core.server.ThreadNames.LEADER_REPLICATION_THREAD;
import static io.journalkeeper.core.server.ThreadNames.STATE_MACHINE_THREAD;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
class Leader<E, ER, Q, QR> extends ServerStateMachine implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class);

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

    /**
     * Leader有效期，用于读取状态时判断leader是否还有效，每次从Follower收到心跳响应，定时更新leader的有效期。
     */
    private AtomicLong leaderShipDeadLineMs = new AtomicLong(0L);

    private final Threads threads;
    private final long heartbeatIntervalMs;
    private final int replicationParallelism;
    private final int replicationBatchSize;
    private final long rpcTimeoutMs;
    private final Journal journal;
    /**
     * 存放节点上所有状态快照的稀疏数组，数组的索引（key）就是快照对应的日志位置的索引
     */
    private final Map<Long, State<E, ER, Q, QR>> immutableSnapshots;

    private final URI serverUri;
    private final int currentTerm;
    /**
     * 当前集群配置
     */
    private final AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine;
    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected final State state;
    private JMetric updateClusterStateMetric;
    private JMetric appendJournalMetric;
    private final Map<URI, JMetric> appendEntriesRpcMetricMap;

    private final ServerRpcProvider serverRpcProvider;
    private final ExecutorService asyncExecutor;
    private final ScheduledExecutorService scheduledExecutor;

    private final VoterConfigManager voterConfigManager;
    private final MetricProvider metricProvider;
    private final CheckTermInterceptor checkTermInterceptor;

    private final AtomicBoolean writeEnabled = new AtomicBoolean(true);
    private final Serializer<ER> entryResultSerializer;
    private final JournalEntryParser journalEntryParser;
    Leader(Journal journal, State state, Map<Long, State<E, ER, Q, QR>> immutableSnapshots,
           int currentTerm,
           AbstractServer.VoterConfigurationStateMachine votersConfigStateMachine,
           URI serverUri,
           int cacheRequests, long heartbeatIntervalMs, long rpcTimeoutMs, int replicationParallelism, int replicationBatchSize,
           Serializer<ER> entryResultSerializer,
           Threads threads,
           ServerRpcProvider serverRpcProvider,
           ExecutorService asyncExecutor,
           ScheduledExecutorService scheduledExecutor, VoterConfigManager voterConfigManager, MetricProvider metricProvider, CheckTermInterceptor checkTermInterceptor, JournalEntryParser journalEntryParser) {

        super(true);
        this.pendingUpdateStateRequests = new LinkedBlockingQueue<>(cacheRequests);
        this.state = state;
        this.votersConfigStateMachine = votersConfigStateMachine;
        this.serverUri = serverUri;
        this.replicationParallelism = replicationParallelism;
        this.replicationBatchSize = replicationBatchSize;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.currentTerm = currentTerm;
        this.immutableSnapshots = immutableSnapshots;
        this.threads = threads;
        this.serverRpcProvider = serverRpcProvider;
        this.asyncExecutor = asyncExecutor;
        this.scheduledExecutor = scheduledExecutor;
        this.voterConfigManager = voterConfigManager;
        this.metricProvider = metricProvider;
        this.checkTermInterceptor = checkTermInterceptor;
        this.journalEntryParser = journalEntryParser;
        this.replicationCallbacks = new RingBufferBelt(rpcTimeoutMs, cacheRequests);
        this.flushCallbacks = new RingBufferBelt(rpcTimeoutMs, cacheRequests);
        this.appendEntriesRpcMetricMap = new HashMap<>(2);
        this.entryResultSerializer = entryResultSerializer;
        this.journal = journal;
        this.heartbeatIntervalMs = heartbeatIntervalMs;

    }

    private AsyncLoopThread buildLeaderAppendJournalEntryThread() {
        return ThreadBuilder.builder()
                .name(LEADER_APPEND_ENTRY_THREAD)
                .doWork(this::appendJournalEntry)
                .sleepTime(0, 0)
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_APPEND_ENTRY_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildLeaderReplicationThread() {
        return ThreadBuilder.builder()
                .name(LEADER_REPLICATION_THREAD)
                .doWork(this::replication)
                .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_REPLICATION_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildLeaderReplicationResponseThread() {
        return ThreadBuilder.builder()
                .name(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD)
                .doWork(this::leaderUpdateCommitIndex)
                .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_REPLICATION_RESPONSES_HANDLER_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildCallbackThread() {
        return ThreadBuilder.builder()
                .name(LEADER_CALLBACK_THREAD)
                .doWork(this::callback)
                .sleepTime(heartbeatIntervalMs, heartbeatIntervalMs)
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_CALLBACK_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    /**
     * 串行写入日志
     */
    private void appendJournalEntry() throws Exception {

        UpdateStateRequestResponse rr = pendingUpdateStateRequests.take();
        final UpdateClusterStateRequest request = rr.getRequest();
        final ResponseFuture responseFuture = rr.getResponseFuture();
        try {

            if(voterConfigManager.maybeUpdateLeaderConfig(request,
                    votersConfigStateMachine,journal, () -> doAppendJournalEntryCallable(request, responseFuture),
                    serverUri, followers, replicationParallelism, appendEntriesRpcMetricMap)) {
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

        JournalEntry entry ;

        if(request.isIncludeHeader()) {
            entry = journalEntryParser.parse(request.getEntry());
        } else {
            entry = journalEntryParser.createJournalEntry(request.getEntry());
        }
        entry.setPartition(request.getPartition());
        entry.setBatchSize(request.getBatchSize());
        entry.setTerm(currentTerm);
        long index = journal.append(entry);
        if (request.getResponseConfig() == ResponseConfig.REPLICATION ) {
            replicationCallbacks.put(new Callback(index, responseFuture.getReplicationFuture()));
        } else if (request.getResponseConfig() == ResponseConfig.PERSISTENCE ) {
            flushCallbacks.put(new Callback(index, responseFuture.getFlushFuture()));
        } else if (request.getResponseConfig() == ResponseConfig.ALL) {
            replicationCallbacks.put(new Callback(index, responseFuture.getReplicationFuture()));
            flushCallbacks.put(new Callback(index, responseFuture.getFlushFuture()));
        }
        threads.wakeupThread(LEADER_REPLICATION_THREAD);
        threads.wakeupThread(FLUSH_JOURNAL_THREAD);
        appendJournalMetric.end(request.getEntry().length);
    }

    /**
     * 反复检查每个FOLLOWER的下一条复制位置nextIndex和本地日志log[]的最大位置，
     * 如果存在差异，发送asyncAppendEntries请求，同时更新对应FOLLOWER的nextIndex。
     * 复制发送线程只负责发送asyncAppendEntries请求，不处理响应。
     */
    private void replication() {
        // 如果是单节点，直接唤醒leaderReplicationResponseHandlerThread，减少响应时延。
        if (serverState() == ServerState.RUNNING && followers.isEmpty()) {
            threads.wakeupThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
            leaderShipDeadLineMs.set(System.currentTimeMillis() + heartbeatIntervalMs);
        }

        boolean hasData;
        do {
            hasData = false;

            for (ReplicationDestination follower : followers) {
                long maxIndex = journal.maxIndex();
                if (follower.nextIndex < maxIndex) {
                    List<byte[]> entries = journal.readRaw(follower.getNextIndex(), this.replicationBatchSize);
                    AsyncAppendEntriesRequest request =
                            new AsyncAppendEntriesRequest(currentTerm, serverUri,
                                    follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                    entries, journal.commitIndex(), maxIndex);
                    sendAppendEntriesRequest(follower, request);
                    follower.setNextIndex(follower.getNextIndex() + entries.size());
                    hasData = true;
                } else {
                    // Send heartbeat
                    if (System.currentTimeMillis() - follower.getLastHeartbeatRequestTime() >= heartbeatIntervalMs) {
                        AsyncAppendEntriesRequest request =
                                new AsyncAppendEntriesRequest(currentTerm, serverUri,
                                        follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                        Collections.emptyList(), journal.commitIndex(), maxIndex);
                        sendAppendEntriesRequest(follower, request);
                    }
                }
            }

        } while (serverState() == ServerState.RUNNING &&
                !Thread.currentThread().isInterrupted() &&
                hasData);

    }


    private void sendAppendEntriesRequest(ReplicationDestination follower, AsyncAppendEntriesRequest request) {
        if (serverState() != ServerState.RUNNING || request.getTerm() != currentTerm) {
            logger.warn("Drop AsyncAppendEntries Request: follower: {}, term: {}, " +
                            "prevLogIndex: {}, prevLogTerm: {}, entries: {}, " +
                            "leader: {}, leaderCommit: {}, {}.",
                    follower.getUri(), request.getTerm(), request.getPrevLogIndex(),
                    request.getPrevLogTerm(), request.getEntries().size(),
                    request.getLeader(), request.getLeaderCommit(),
                    voterInfo());
            return;
        }
        if (logger.isDebugEnabled()) {
            final boolean isHeartbeat = null == request.getEntries() || request.getEntries().isEmpty();
            logger.debug("Send {}, " +
                            "follower: {}, " +
                            "term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                            "entries: {}, leaderCommit: {}, {}.",
                    isHeartbeat ? "heartbeat": "appendEntriesRequest",
                    follower.getUri(),
                    request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                    request.getEntries().size(), request.getLeaderCommit(), voterInfo());
        }
        follower.setLastHeartbeatRequestTime(System.currentTimeMillis());

        serverRpcProvider.getServerRpc(follower.getUri())
                .thenCompose(serverRpc -> serverRpc.asyncAppendEntries(request))
                .exceptionally(AsyncAppendEntriesResponse::new)
                .thenAccept(response -> leaderOnAppendEntriesResponse(follower, request, response));


    }


    private void leaderOnAppendEntriesResponse(ReplicationDestination follower, AsyncAppendEntriesRequest request, AsyncAppendEntriesResponse response) {
        if(checkTermInterceptor.checkTerm(response.getTerm())) {
            return;
        }

        if (response.success()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Update lastHeartbeatResponseTime of {}, {}.", follower.getUri(), voterInfo());
            }
            follower.setLastHeartbeatResponseTime(System.currentTimeMillis());

            // 计算leader有效期
            long [] sortedHeartbeatResponseTimes = followers.stream().mapToLong(ReplicationDestination::getLastHeartbeatResponseTime)
                    .sorted().toArray();

            leaderShipDeadLineMs.set(
                    (sortedHeartbeatResponseTimes[sortedHeartbeatResponseTimes.length / 2]) + heartbeatIntervalMs) ;



            if (response.getTerm() == currentTerm) {
                if (response.getEntryCount() > 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Handle appendEntriesResponse, success: {}, journalIndex: {}, " +
                                        "entryCount: {}, term: {}, follower: {}, {}.",
                                response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                                follower.getUri(), voterInfo());
                    }
                    follower.addResponse(response);
                    threads.wakeupThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Ignore heartbeat response, success: {}, journalIndex: {}, " +
                                    "entryCount: {}, term: {}, follower: {}, {}.",
                            response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                            follower.getUri(), voterInfo());
                }

            } else {
                logger.warn("Drop outdated AsyncAppendEntries Response: follower: {}, term: {}, index: {}, {}.",
                        follower.getUri(), response.getTerm(), response.getJournalIndex(), voterInfo());
            }
        } else if(request.getEntries().size() > 0 ){ // 心跳不重试
            logger.warn("Replication response error: {}", response.errorString());
            delaySendAsyncAppendEntriesRpc(follower, request);
        }
    }

    private void delaySendAsyncAppendEntriesRpc(ReplicationDestination follower, AsyncAppendEntriesRequest request) {
        new Timer("Retry-AsyncAppendEntriesRpc", true).schedule(new TimerTask() {
            @Override
            public void run() {
                sendAppendEntriesRequest(follower, request);
            }
        }, heartbeatIntervalMs);
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
    private void leaderUpdateCommitIndex() throws InterruptedException, ExecutionException, IOException {
        List<ReplicationDestination> finalFollowers = new ArrayList<>(followers);
        long N = 0L;
        if (finalFollowers.isEmpty()) {
            N = journal.maxIndex();
        } else {


            List<Callable<Boolean>> callables =
                    finalFollowers.stream()
                            .map(follower -> (Callable<Boolean>) () -> leaderHandleAppendEntriesResponse(follower))
                            .collect(Collectors.toList());

            boolean isAnyFollowerMatchIndexUpdated = false;
            for (Future<Boolean> future : asyncExecutor.invokeAll(callables)) {
                if (future.get()) {
                    isAnyFollowerMatchIndexUpdated = true;
                    break;
                }
            }

            if (isAnyFollowerMatchIndexUpdated) {
                if (votersConfigStateMachine.isJointConsensus()) {
                    long[] sortedMatchIndexInOldConfig = finalFollowers.stream()
                            .filter(follower -> votersConfigStateMachine.getConfigOld().contains(follower.getUri()))
                            .mapToLong(ReplicationDestination::getMatchIndex)
                            .sorted().toArray();
                    long nInOldConfig = sortedMatchIndexInOldConfig.length > 0 ?
                            sortedMatchIndexInOldConfig[sortedMatchIndexInOldConfig.length / 2] : journal.maxIndex();

                    long[] sortedMatchIndexInNewConfig = finalFollowers.stream()
                            .filter(follower -> votersConfigStateMachine.getConfigNew().contains(follower.getUri()))
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
        if (N > journal.commitIndex() && journal.getTerm(N - 1) == currentTerm) {
            if (logger.isDebugEnabled()) {
                logger.debug("Set commitIndex {} to {}, {}.", journal.commitIndex(), N, voterInfo());
            }
            journal.commit(N);
            onCommitted();
        }
    }

    private void onCommitted() {
        // 唤醒状态机线程
        threads.wakeupThread(STATE_MACHINE_THREAD);
        threads.wakeupThread(LEADER_REPLICATION_THREAD);
    }

    /**
     * 处理follower中所有AppendEntriesRPC的响应
     * @param follower follower
     * @return 是否更新了follower的matchIndex
     */
    private boolean leaderHandleAppendEntriesResponse(ReplicationDestination follower) {
        boolean isMatchIndexUpdated = false;
        AsyncAppendEntriesResponse response;
        while (serverState() == ServerState.RUNNING &&
                (response = follower.pendingResponses.poll()) != null) {
            if (response.getEntryCount() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received appendEntriesResponse, success: {}, journalIndex: {}, " +
                                    "entryCount: {}, term: {}, follower: {}, {}.",
                            response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                            follower.getUri(), voterInfo());
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received heartbeat response, success: {}, journalIndex: {}, " +
                                    "entryCount: {}, term: {}, follower: {}, {}.",
                            response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                            follower.getUri(), voterInfo());
                }
            }
            if (currentTerm == response.getTerm()) {
                if (response.isSuccess()) {

                    if (follower.getRepStartIndex() == response.getJournalIndex()) {
                        follower.setRepStartIndex(follower.getRepStartIndex() + response.getEntryCount());
                        follower.setMatchIndex(response.getJournalIndex() + response.getEntryCount());
                        isMatchIndexUpdated = true;
                        if (response.getEntryCount() > 0) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Replication success, RepStartIndex: {}, matchIndex: {}, follower: {}, {}.",
                                        follower.getRepStartIndex(), follower.getMatchIndex(), follower.getUri(),
                                        voterInfo());
                            }
                        }
                    } else {
                        // 出现空洞，重新放回队列，等待后续处理
                        follower.addResponse(response);
                        break;
                    }
                } else if (response.getEntryCount() > 0) {
                    // 失败且不是心跳
                    if (follower.getRepStartIndex() == response.getJournalIndex()) {
                        // 需要回退
                        int rollbackSize = (int) Math.min(replicationBatchSize, follower.repStartIndex - journal.minIndex());
                        follower.repStartIndex -= rollbackSize;
                        sendAppendEntriesRequest(follower,
                                new AsyncAppendEntriesRequest(currentTerm, serverUri,
                                        follower.repStartIndex - 1,
                                        journal.getTerm(follower.repStartIndex - 1),
                                        journal.readRaw(follower.repStartIndex, rollbackSize),
                                        journal.commitIndex(), journal.maxIndex()));
                    }
                    delaySendAsyncAppendEntriesRpc(follower, new AsyncAppendEntriesRequest(currentTerm, serverUri,
                            response.getJournalIndex() - 1,
                            journal.getTerm(response.getJournalIndex() - 1),
                            journal.readRaw(response.getJournalIndex(), response.getEntryCount()),
                            journal.commitIndex(), journal.maxIndex()));
                }
            }
        }
        return isMatchIndexUpdated;
    }

    private void callback() {
//        replicationCallbacks.callbackBefore(state.lastApplied());
        flushCallbacks.callbackBefore(journalFlushIndex.get());
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
            if(serverState() != ServerState.RUNNING) {
                throw new IllegalStateException(String.format("Leader is not RUNNING, state: %s.", serverState().toString()));
            }

            if(!writeEnabled.get()) {
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
        if(currentTerm != term) {
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
        this.followers.addAll(this.votersConfigStateMachine.voters().stream()
                .filter(uri -> !uri.equals(serverUri))
                .map(uri -> new ReplicationDestination(uri, journal.maxIndex(), replicationParallelism))
                .collect(Collectors.toList()));

        this.followers.forEach(follower ->
                appendEntriesRpcMetricMap.put(
                        follower.getUri(),
                        metricProvider.getMetric(getMetricName(METRIC_APPEND_ENTRIES_RPC, follower.getUri()))));
        this.appendJournalMetric = metricProvider.getMetric(MetricNames.METRIC_APPEND_JOURNAL);
        this.updateClusterStateMetric = metricProvider.getMetric(MetricNames.METRIC_UPDATE_CLUSTER_STATE);


        this.threads.createThread(buildLeaderAppendJournalEntryThread());
        this.threads.createThread(buildLeaderReplicationThread());
        this.threads.createThread(buildLeaderReplicationResponseThread());
        this.threads.createThread(buildCallbackThread());
        this.threads.startThread(LEADER_APPEND_ENTRY_THREAD);
        this.threads.startThread(LEADER_CALLBACK_THREAD);
        this.threads.startThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
        this.threads.startThread(LEADER_REPLICATION_THREAD);
    }

    @Override
    protected void doStop() {
        super.doStop();
        mayBeWaitingForAppendJournals();
        this.threads.stopThread(LEADER_APPEND_ENTRY_THREAD);
        this.threads.stopThread(LEADER_CALLBACK_THREAD);
        failAllPendingCallbacks();
        this.threads.stopThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
        this.threads.stopThread(LEADER_REPLICATION_THREAD);
        this.threads.removeThread(LEADER_APPEND_ENTRY_THREAD);
        this.threads.removeThread(LEADER_CALLBACK_THREAD);
        this.threads.removeThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
        this.threads.removeThread(LEADER_REPLICATION_THREAD);
        removeAppendEntriesRpcMetrics();
        metricProvider.removeMetric(MetricNames.METRIC_APPEND_JOURNAL);
        metricProvider.removeMetric(MetricNames.METRIC_UPDATE_CLUSTER_STATE);
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
        appendEntriesRpcMetricMap.forEach((followerUri, metric) -> {
            metricProvider.removeMetric(getMetricName(METRIC_APPEND_ENTRIES_RPC, followerUri));
        });
    }

    private static String getMetricName(String prefix, URI followerUri) {
        return prefix + "_" + followerUri.toString();
    }


    void callback(long lastApplied, ER result)  {
        replicationCallbacks.callback(lastApplied, entryResultSerializer.serialize(result));
    }

    void onJournalFlushed() {

        journalFlushIndex.set(journal.maxIndex());
        if(serverState() == ServerState.RUNNING) {
            threads.wakeupThread(LEADER_APPEND_ENTRY_THREAD);
            threads.wakeupThread(LEADER_CALLBACK_THREAD);
        }

    }
    /**
     * 异步检测Leader有效性，成功返回null，失败抛出异常。
     */
    CompletableFuture<Void> waitLeadership() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                    long start = System.currentTimeMillis();
                    while (!checkLeadership()) {

                        if (System.currentTimeMillis() - start > rpcTimeoutMs) {
                            throw new TimeoutException();
                        }
                        Thread.sleep(heartbeatIntervalMs / 10);

                    }
                    completableFuture.complete(null);

            } catch (InterruptedException e) {
                completableFuture.completeExceptionally(e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        }, asyncExecutor);
        return completableFuture;
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
    boolean checkLeadership() {

        long now = System.currentTimeMillis();
        return now <= leaderShipDeadLineMs.get();
    }

    private static class UpdateStateRequestResponse {
        private final UpdateClusterStateRequest request;
        private final ResponseFuture responseFuture;
        private final long start = System.nanoTime();
        private long logIndex;

        UpdateStateRequestResponse(UpdateClusterStateRequest request, JMetric metric) {
            this.request = request;
            responseFuture = new ResponseFuture(request.getResponseConfig());
            final int length = request.getEntry().length;
            responseFuture.getResponseFuture()
                    .thenRun(() -> metric.mark(System.nanoTime() - start, length));

        }

        UpdateClusterStateRequest getRequest() {
            return request;
        }

        ResponseFuture getResponseFuture() {
            return this.responseFuture;
        }
        public long getLogIndex() {
            return logIndex;
        }

        public void setLogIndex(long logIndex) {
            this.logIndex = logIndex;
        }
    }


    static class ReplicationDestination {


        private final URI uri;
        /**
         * 仅LEADER使用，待处理的asyncAppendEntries Response，按照Response中的logIndex排序。
         */
        private final BlockingQueue<AsyncAppendEntriesResponse> pendingResponses;
        /**
         * 需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
         */
        private long nextIndex;
        /**
         * 已经复制到该服务器的日志的最高索引值（从 0 开始递增）
         */
        private long matchIndex = 0L;
        /**
         * 所有在途的日志复制请求中日志位置的最小值（初始化为nextIndex）
         */
        // TODO: 删除日志的时候不能超过repStartIndex。
        private long repStartIndex;
        /**
         * 上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
         */
        private long lastHeartbeatResponseTime;
        private long lastHeartbeatRequestTime = 0L;

        ReplicationDestination(URI uri, long nextIndex, int replicationParallelism) {
            this.uri = uri;
            this.nextIndex = nextIndex;
            this.repStartIndex = nextIndex;
            this.lastHeartbeatResponseTime = 0L;
            pendingResponses =
                    new PriorityBlockingQueue<>(replicationParallelism * 2,
                            Comparator.comparing(AsyncAppendEntriesResponse::getTerm)
                                    .thenComparing(AsyncAppendEntriesResponse::getJournalIndex)
                    );
        }


        URI getUri() {
            return uri;
        }

        long getNextIndex() {
            return nextIndex;
        }

        void setNextIndex(long nextIndex) {
            this.nextIndex = nextIndex;
        }

        long getMatchIndex() {
            return matchIndex;
        }

        void setMatchIndex(long matchIndex) {
            this.matchIndex = matchIndex;
        }

        long getRepStartIndex() {
            return repStartIndex;
        }

        void setRepStartIndex(long repStartIndex) {
            this.repStartIndex = repStartIndex;
        }

        long getLastHeartbeatResponseTime() {
            return lastHeartbeatResponseTime;
        }

        void setLastHeartbeatResponseTime(long lastHeartbeatResponseTime) {
            this.lastHeartbeatResponseTime = lastHeartbeatResponseTime;
        }

        void addResponse(AsyncAppendEntriesResponse response) {
            pendingResponses.add(response);
        }

        public long getLastHeartbeatRequestTime() {
            return lastHeartbeatRequestTime;
        }

        public void setLastHeartbeatRequestTime(long lastHeartbeatRequestTime) {
            this.lastHeartbeatRequestTime = lastHeartbeatRequestTime;
        }
    }

    private final static class ResponseFuture {
        private final CompletableFuture<UpdateClusterStateResponse> responseFuture;
        private final CompletableFuture<UpdateClusterStateResponse> flushFuture;
        private final CompletableFuture<UpdateClusterStateResponse> replicationFuture;
        private ResponseFuture(ResponseConfig responseConfig) {
            this.responseFuture = new CompletableFuture<>();
            if(responseConfig == ResponseConfig.ALL) {
                this.flushFuture = new CompletableFuture<>();
                this.replicationFuture = new CompletableFuture<>();

                this.replicationFuture.whenComplete((replicationResponse, e) -> {
                    if (null == e) {
                        if (replicationResponse.success()) {
                            this.flushFuture.whenComplete((flushResponse, t) -> {
                                if (null == t) {
                                    if (flushResponse.success()) {
                                        // 如果都成功，优先使用replication response，因为里面有执行状态机的返回值。
                                        responseFuture.complete(replicationResponse);
                                    } else {
                                        // replication 成功，flush 失败，返回失败的flush response。
                                        responseFuture.complete(flushResponse);
                                    }
                                } else {
                                    responseFuture.complete(new UpdateClusterStateResponse(t));
                                }
                            });
                        } else {
                            responseFuture.complete(replicationResponse);
                        }
                    } else {
                        responseFuture.complete(new UpdateClusterStateResponse(e));
                    }
                });
            } else {
                this.flushFuture = responseFuture;
                this.replicationFuture = responseFuture;
            }
        }

        private CompletableFuture<UpdateClusterStateResponse> getResponseFuture() {
            return responseFuture;
        }

        private CompletableFuture<UpdateClusterStateResponse> getFlushFuture() {
            return flushFuture;
        }

        private CompletableFuture<UpdateClusterStateResponse> getReplicationFuture() {
            return replicationFuture;
        }
    }
}
