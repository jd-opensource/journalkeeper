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

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.entry.Entry;
import io.journalkeeper.core.entry.EntryHeader;
import io.journalkeeper.core.entry.reserved.LeaderAnnouncementEntry;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ReservedEntry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS1Entry;
import io.journalkeeper.core.entry.reserved.UpdateVotersS2Entry;
import io.journalkeeper.core.exception.UpdateConfigurationException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.rpc.client.UpdateVotersResponse;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
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
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITION;


/**
 * @author LiYue
 * Date: 2019-03-18
 */
public class Voter<E, ER, Q, QR> extends Server<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(Voter.class);
    /**
     * Leader接收客户端请求串行写入entries线程
     */
    private static final String LEADER_APPEND_ENTRY_THREAD = "LeaderAppendEntryThread";
    /**
     * Voter 处理AppendEntriesRequest线程
     */
    private static final String VOTER_REPLICATION_REQUESTS_HANDLER_THREAD = "VoterReplicationRequestHandlerThread";
    /**
     * Voter 处理回调线程
     */
    private static final String LEADER_CALLBACK_THREAD = "LeaderCallbackThread";
    /**
     * Leader 发送AppendEntries RPC线程
     */
    private static final String LEADER_REPLICATION_THREAD = "LeaderReplicationThread";
    /**
     * Leader 处理AppendEntries RPC Response 线程
     */
    private static final String LEADER_REPLICATION_RESPONSES_HANDLER_THREAD = "LeaderReplicationResponsesHandlerThread";

    private final static String METRIC_UPDATE_CLUSTER_STATE = "UPDATE_CLUSTER_STATE";
    private final static String METRIC_APPEND_JOURNAL = "APPEND_JOURNAL";
    private final static String METRIC_APPEND_ENTRIES_RPC = "APPEND_ENTRIES_RPC";

    private final JMetric updateClusterStateMetric;
    private final JMetric appendJournalMetric;
    private final Map<URI, JMetric> appendEntriesRpcMetricMap;
    /**
     * Voter最后知道的任期号（从 0 开始递增）
     */
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    // LEADER ONLY
    /**
     * 串行处理所有RequestVoterRPC request/response
     */
    private final Object voteRequestMutex = new Object();
    private final Object voteResponseMutex = new Object();
    /**
     * 串行处理所有角色变更和配置变更
     */
    private final Object rollMutex = new Object();
    private final BlockingQueue<UpdateStateRequestResponse> pendingUpdateStateRequests;
    private final Config config;

    private final CallbackBelt<ER> replicationCallbacks;
    private final CallbackBelt<ER> flushCallbacks;
    /**
     * 刷盘位置
     */
    private final AtomicLong journalFlushIndex = new AtomicLong(0L);
    /**
     * 选民状态，在LEADER、FOLLOWER和CANDIDATE之间转换。初始值为FOLLOWER。
     */
    private final VoterStateMachine voterState = new VoterStateMachine();
    /**
     * 在当前任期内收到选票的候选人地址（如果没有就为 null）
     */
    private URI votedFor = null;
    /**
     * 选举（心跳）超时
     */
    private long electionTimeoutMs;
    /**
     * 当角色为LEADER时，记录所有FOLLOWER的位置等信息
     */
    private final List<Follower> followers = new CopyOnWriteArrayList<>();
    /**
     * 上次从LEADER收到心跳（asyncAppendEntries）的时间戳
     */
    private long lastHeartbeat;
    /**
     * 待处理的asyncAppendEntries Request，按照request中的preLogTerm和prevLogIndex排序。
     */
    private BlockingQueue<ReplicationRequestResponse> pendingAppendEntriesRequests;
    private ScheduledFuture checkElectionTimeoutFuture;

    /**
     * Leader有效期，用于读取状态时判断leader是否还有效，每次从Follower收到心跳响应，定时更新leader的有效期。
     */
    private AtomicLong leaderShipDeadLineMs = new AtomicLong(0L);



    public Voter(StateFactory<E, ER, Q, QR> stateFactory, Serializer<E> entrySerializer, Serializer<ER> entryResultSerializer,
                 Serializer<Q> querySerializer, Serializer<QR> resultSerializer,
                 ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {
        super(stateFactory, entrySerializer, entryResultSerializer, querySerializer, resultSerializer, scheduledExecutor, asyncExecutor, properties);
        this.config = toConfig(properties);

        replicationCallbacks = new LinkedQueueCallbackBelt<>(config.getRpcTimeoutMs(), entryResultSerializer);
        flushCallbacks = new LinkedQueueCallbackBelt<>(config.getRpcTimeoutMs(), entryResultSerializer);
        electionTimeoutMs = randomInterval(config.getElectionTimeoutMs());

        pendingUpdateStateRequests = new LinkedBlockingQueue<>(config.getCacheRequests());
        pendingAppendEntriesRequests = new PriorityBlockingQueue<>(config.getCacheRequests(),
                Comparator.comparing(ReplicationRequestResponse::getPrevLogTerm)
                        .thenComparing(ReplicationRequestResponse::getPrevLogIndex));


        threads.createThread(buildLeaderAppendJournalEntryThread());
        threads.createThread(buildVoterReplicationHandlerThread());
        threads.createThread(buildLeaderReplicationThread());
        threads.createThread(buildLeaderReplicationResponseThread());
        threads.createThread(buildCallbackThread());

        this.updateClusterStateMetric = getMetric(METRIC_UPDATE_CLUSTER_STATE);
        this.appendJournalMetric = getMetric(METRIC_APPEND_JOURNAL);
        this.appendEntriesRpcMetricMap = new HashMap<>(2);
    }

    private AsyncLoopThread buildLeaderAppendJournalEntryThread() {
        return ThreadBuilder.builder()
                .name(LEADER_APPEND_ENTRY_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::appendJournalEntry)
                .sleepTime(0, 0)
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_APPEND_ENTRY_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildVoterReplicationHandlerThread() {
        return ThreadBuilder.builder()
                .name(VOTER_REPLICATION_REQUESTS_HANDLER_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::followerHandleAppendEntriesRequest)
                .sleepTime(config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("{} Exception, {}: ", VOTER_REPLICATION_REQUESTS_HANDLER_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildLeaderReplicationThread() {
        return ThreadBuilder.builder()
                .name(LEADER_REPLICATION_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::replication)
                .sleepTime(config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_REPLICATION_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildLeaderReplicationResponseThread() {
        return ThreadBuilder.builder()
                .name(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING && this.voterState() == VoterState.LEADER)
                .doWork(this::leaderUpdateCommitIndex)
                .sleepTime(config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_REPLICATION_RESPONSES_HANDLER_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildCallbackThread() {
        return ThreadBuilder.builder()
                .name(LEADER_CALLBACK_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::callback)
                .sleepTime(config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("{} Exception, {}: ", LEADER_CALLBACK_THREAD, voterInfo(), e))
                .daemon(true)
                .build();
    }

    @Override
    protected void onPrintMetric() {
        super.onPrintMetric();
        logger.info("PendingUpdateRequests: {}, {}.", pendingUpdateStateRequests.size(), voterInfo());
        logger.info("PendingCallbacks: flush: {}, replication: {}.", flushCallbacks.size(), replicationCallbacks.size());
    }

    private void callback() {
        replicationCallbacks.callbackBefore(state.lastApplied());
        flushCallbacks.callbackBefore(journalFlushIndex.get());
    }

    /**
     * 串行写入日志
     */
    private void appendJournalEntry() throws Exception {

        UpdateStateRequestResponse rr = pendingUpdateStateRequests.take();
        final UpdateClusterStateRequest request = rr.getRequest();
        final CompletableFuture<UpdateClusterStateResponse> responseFuture = rr.getResponseFuture();
        if (voterState() == VoterState.LEADER) {
            try {
                if (maybeUpdateLeaderConfig(request, responseFuture)) {
                    return;
                }

                doAppendJournalEntry(request, responseFuture);

            } catch (Throwable t) {
                responseFuture.complete(new UpdateClusterStateResponse(t));
                throw t;
            }
        } else {
            logger.warn("NOT_LEADER!");
            rr.getResponseFuture().complete(new UpdateClusterStateResponse(new NotLeaderException(leader)));
        }
    }


    private boolean maybeUpdateLeaderConfig(UpdateClusterStateRequest request, CompletableFuture<UpdateClusterStateResponse> responseFuture) throws Exception {
        // 如果是配置变更请求，立刻更新当前配置
        if(request.getPartition() == RESERVED_PARTITION ){
            int entryType = ReservedEntriesSerializeSupport.parseEntryType(request.getEntry());
            if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                UpdateVotersS1Entry updateVotersS1Entry = ReservedEntriesSerializeSupport.parse(request.getEntry());
                // 等待所有日志都被提交才能进行配置变更
                waitingForAllEntriesCommitted();

                votersConfigStateMachine.toJointConsensus(updateVotersS1Entry.getConfigNew(),
                        () -> doAppendJournalEntryCallable(request, responseFuture));
                List<URI> oldFollowerUriList = followers.stream().map(Follower::getUri).collect(Collectors.toList());
                for (URI uri : updateVotersS1Entry.getConfigNew()) {
                    if (!serverUri().equals(uri) && // uri was not me
                            !oldFollowerUriList.contains(uri)) { // and not included in the old followers collection
                        followers.add(new Follower(uri, journal.maxIndex(), config.getReplicationParallelism()));
                    }
                }
                return true;
            } else if (entryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                // 等待所有日志都被提交才能进行配置变更
                waitingForAllEntriesCommitted();
                votersConfigStateMachine.toNewConfig(() -> doAppendJournalEntryCallable(request, responseFuture));


                followers.removeIf(follower -> {
                    if(!votersConfigStateMachine.voters().contains(follower.getUri())){
                        appendEntriesRpcMetricMap.remove(follower.getUri());
                        return true;
                    } else {
                        return false;
                    }
                });
                return true;
            }
        }
        return false;
    }

    /**
     * Block current thread and waiting until all entries were committed。
     */
    private void waitingForAllEntriesCommitted() throws InterruptedException {
        long t0 = System.nanoTime();
        while(journal.commitIndex() < journal.maxIndex()) {
            if(System.nanoTime() - t0 < 10000000000L) {
                Thread.yield();
            } else {
                Thread.sleep(10L);
            }
        }
    }

    private Void doAppendJournalEntryCallable(UpdateClusterStateRequest request, CompletableFuture<UpdateClusterStateResponse> responseFuture) throws InterruptedException {
        doAppendJournalEntry(request, responseFuture);
        return null;
    }

    private void doAppendJournalEntry(UpdateClusterStateRequest request, CompletableFuture<UpdateClusterStateResponse> responseFuture) throws InterruptedException {
        mayBeWaitingForApplyEntries();
        appendJournalMetric.start();

        long index = journal.append(new Entry(request.getEntry(), currentTerm.get(), request.getPartition(), request.getBatchSize()));
        if (request.getResponseConfig() == ResponseConfig.PERSISTENCE) {
            flushCallbacks.put(new Callback<>(index - 1, responseFuture));
        } else if (request.getResponseConfig() == ResponseConfig.REPLICATION) {
            replicationCallbacks.put(new Callback<>(index - 1, responseFuture));
        }
        appendJournalMetric.end(request.getEntry().length);
    }


    private void mayBeWaitingForApplyEntries() throws InterruptedException {
        long t0 = System.nanoTime();
        while (journal.maxIndex() - state.lastApplied() > config.getCacheRequests()) {
            if(System.nanoTime() - t0 < 10000000000L) {
                Thread.yield();
            } else {
                Thread.sleep(10);
            }
        }
    }

    @Override
    protected void onJournalFlushed() {

        journalFlushIndex.set(journal.maxIndex());
        if (threads.getTreadState(LEADER_APPEND_ENTRY_THREAD) == ServerState.RUNNING) {
            threads.wakeupThread(LEADER_APPEND_ENTRY_THREAD);
        }
        if (threads.getTreadState(LEADER_CALLBACK_THREAD) == ServerState.RUNNING) {
            threads.wakeupThread(LEADER_CALLBACK_THREAD);
        }
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setElectionTimeoutMs(Long.parseLong(
                properties.getProperty(
                        Config.ELECTION_TIMEOUT_KEY,
                        String.valueOf(Config.DEFAULT_ELECTION_TIMEOUT_MS))));
        config.setHeartbeatIntervalMs(Long.parseLong(
                properties.getProperty(
                        Config.HEARTBEAT_INTERVAL_KEY,
                        String.valueOf(Config.DEFAULT_HEARTBEAT_INTERVAL_MS))));
        config.setReplicationBatchSize(Integer.parseInt(
                properties.getProperty(
                        Config.REPLICATION_BATCH_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_REPLICATION_BATCH_SIZE))));
        config.setReplicationParallelism(Integer.parseInt(
                properties.getProperty(
                        Config.REPLICATION_PARALLELISM_KEY,
                        String.valueOf(Config.DEFAULT_REPLICATION_PARALLELISM))));
        config.setCacheRequests(Integer.parseInt(
                properties.getProperty(
                        Config.CACHE_REQUESTS_KEY,
                        String.valueOf(Config.DEFAULT_CACHE_REQUESTS))));
        return config;
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

        long now = System.currentTimeMillis();
        return now <= leaderShipDeadLineMs.get();
    }

    private void checkElectionTimeout() {
        try {
            if (voterState() != VoterState.LEADER && System.currentTimeMillis() - lastHeartbeat > electionTimeoutMs) {
                startElection();
            }
        } catch (Throwable t) {
            logger.warn("CheckElectionTimeout Exception, {}: ", voterInfo(), t);
        }
    }

    /**
     * 发起选举。
     * 0. 角色转变为候选人
     * 1. 自增当前任期号：term = term + 1；
     * 2. 给自己投票；
     * 3. 重置选举计时器：lastHeartBeat = now，生成一个随机的新的选举超时时间（RAFT的推荐值为150~300ms）。
     * 4. 向其他Voter发送RequestVote请求
     * 4.1. 如果收到了来自大多数服务器的投票：成为LEADER
     * 4.2. 如果收到了来自新领导人的asyncAppendEntries请求（heartbeat）：转换状态为FOLLOWER
     * 4.3. 如果选举超时：开始新一轮的选举
     */
    private void startElection() {
        logger.info("Start election, {}", voterInfo());
        convertToCandidate();


        lastHeartbeat = System.currentTimeMillis();
        long lastLogIndex = journal.maxIndex() - 1;
        int lastLogTerm = journal.getTerm(lastLogIndex);

        RequestVoteRequest request = new RequestVoteRequest(currentTerm.get(), uri, lastLogIndex, lastLogTerm);

        List<RequestVoteResponse> responses =
        votersConfigStateMachine.voters().parallelStream()
            .filter(uri -> !uri.equals(this.uri))
            .map(uri -> {
                try {
                    return getServerRpc(uri);
                } catch (Throwable t) {
                    return null;
                }
            }).filter(Objects::nonNull)
            .map(serverRpc -> {
                RequestVoteResponse response;
                try {
                    logger.info("Request vote, dest uri: {}, {}...", serverRpc.serverUri(), voterInfo());
                    response = serverRpc.requestVote(request).get();
                    if(response.success()) {
                        checkTerm(response.getTerm());
                    }
                    logger.info("Request vote result {}, dest uri: {}, {}...",
                            response.isVoteGranted(),
                            serverRpc.serverUri(),
                            voterInfo());
                } catch (Exception e) {
                    logger.info("Request vote exception: {}, dest uri: {}, {}.",
                            e.getCause(), serverRpc.serverUri(), voterInfo());
                    response = new RequestVoteResponse(e);
                }
                response.setUri(serverRpc.serverUri());
                return response;
            }).collect(Collectors.toList());

        long votesGrantedInNewConfig = responses.stream()
            .filter(response -> votersConfigStateMachine.getConfigNew().contains(response.getUri()))
                .filter(RequestVoteResponse::isVoteGranted)
                .filter(response -> response.getTerm() == currentTerm.get())
                .count() + 1;
        boolean winsTheElection;
        if(votersConfigStateMachine.isJointConsensus()) {
            long votesGrantedInOldConfig = responses.stream()
                    .filter(response -> votersConfigStateMachine.getConfigOld().contains(response.getUri()))
                    .filter(RequestVoteResponse::isVoteGranted)
                    .filter(response -> response.getTerm() == currentTerm.get())
                    .count() + 1;
            winsTheElection = votesGrantedInNewConfig >= votersConfigStateMachine.getConfigNew().size() / 2 + 1 &&
                    votesGrantedInOldConfig >= votersConfigStateMachine.getConfigOld().size() / 2 + 1 ;
        } else {
            winsTheElection = votesGrantedInNewConfig >= votersConfigStateMachine.getConfigNew().size() / 2 + 1;
        }

        if(winsTheElection) {
            convertToLeader();
        }
    }

    /**
     * 反复检查每个FOLLOWER的下一条复制位置nextIndex和本地日志log[]的最大位置，
     * 如果存在差异，发送asyncAppendEntries请求，同时更新对应FOLLOWER的nextIndex。
     * 复制发送线程只负责发送asyncAppendEntries请求，不处理响应。
     */
    private void replication() {
        // 如果是单节点，直接唤醒leaderReplicationResponseHandlerThread，减少响应时延。
        if (serverState() == ServerState.RUNNING && voterState() == VoterState.LEADER && followers.isEmpty()) {
            threads.wakeupThread(LEADER_REPLICATION_RESPONSES_HANDLER_THREAD);
        }

        boolean hasData;
        do {
            hasData = false;
            if(followers.size() == 0) {
                leaderShipDeadLineMs.set(System.currentTimeMillis() + config.getHeartbeatIntervalMs());
            }
            for (Follower follower : followers) {
                long maxIndex = journal.maxIndex();
                if (follower.nextIndex < maxIndex) {
                    List<byte[]> entries = journal.readRaw(follower.getNextIndex(), config.getReplicationBatchSize());
                    AsyncAppendEntriesRequest request =
                            new AsyncAppendEntriesRequest(currentTerm.get(), leader,
                                    follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                    entries, journal.commitIndex());
                    sendAppendEntriesRequest(follower, request);
                    follower.setNextIndex(follower.getNextIndex() + entries.size());
                    hasData = true;
                } else {
                    // Send heartbeat
                    if (System.currentTimeMillis() - follower.getLastHeartbeatRequestTime() >= config.getHeartbeatIntervalMs()) {
                        AsyncAppendEntriesRequest request =
                                new AsyncAppendEntriesRequest(currentTerm.get(), leader,
                                        follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                        Collections.emptyList(), journal.commitIndex());
                        sendAppendEntriesRequest(follower, request);
                    }
                }
            }

        } while (serverState() == ServerState.RUNNING &&
                voterState() == VoterState.LEADER &&
                !Thread.currentThread().isInterrupted() &&
                hasData);

    }


    private int getPreLogTerm(long currentLogIndex) {
        if (currentLogIndex > journal.minIndex()) {
            return journal.getTerm(currentLogIndex - 1);
        } else if (currentLogIndex == journal.maxIndex() && snapshots.containsKey(currentLogIndex)) {
            return snapshots.get(currentLogIndex).lastIncludedTerm();
        } else if (currentLogIndex == 0) {
            return -1;
        } else {
            throw new IndexUnderflowException();
        }
    }

    private void sendAppendEntriesRequest(Follower follower, AsyncAppendEntriesRequest request) {
        if (serverState() != ServerState.RUNNING || voterState() != VoterState.LEADER || request.getTerm() != currentTerm.get()) {
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

        CompletableFuture.supplyAsync(() -> getServerRpc(follower.getUri()), asyncExecutor)
                .thenCompose(serverRpc -> serverRpc.asyncAppendEntries(request))
                .exceptionally(AsyncAppendEntriesResponse::new)
                .thenAccept(response -> leaderOnAppendEntriesResponse(follower, request, response));


    }

    private void leaderOnAppendEntriesResponse(Follower follower, AsyncAppendEntriesRequest request, AsyncAppendEntriesResponse response) {
        if (response.success()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Update lastHeartbeatResponseTime of {}, {}.", follower.getUri(), voterInfo());
            }
            follower.setLastHeartbeatResponseTime(System.currentTimeMillis());

            // 计算leader有效期
            long [] sortedHeartbeatResponseTimes = followers.stream().mapToLong(Follower::getLastHeartbeatResponseTime)
                    .sorted().toArray();

            leaderShipDeadLineMs.set(
                    (sortedHeartbeatResponseTimes[sortedHeartbeatResponseTimes.length / 2]) + config.getHeartbeatIntervalMs()) ;

            checkTerm(response.getTerm());
            if (response.getTerm() == currentTerm.get()) {
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
        } else {
            logger.warn("Replication response error: {}", response.errorString());
            delaySendAsyncAppendEntriesRpc(follower, request);
        }
    }

    private void delaySendAsyncAppendEntriesRpc(Follower follower, AsyncAppendEntriesRequest request) {
        new Timer("Retry-AsyncAppendEntriesRpc", true).schedule(new TimerTask() {
            @Override
            public void run() {
                sendAppendEntriesRequest(follower, request);
            }
        }, config.getHeartbeatIntervalMs());
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
        List<Follower> finalFollowers = new ArrayList<>(followers);
        long N = finalFollowers.isEmpty() ? journal.maxIndex() : 0L;

        List<Callable<Boolean>> callables =
                finalFollowers.stream()
                        .map(follower -> (Callable<Boolean>) () -> leaderHandleAppendEntriesResponse(follower))
                        .collect(Collectors.toList());

        boolean isAnyFollowerMatchIndexUpdated = false;
        for (Future<Boolean> future : asyncExecutor.invokeAll(callables)) {
            if(future.get()) {
                isAnyFollowerMatchIndexUpdated = true;
                break;
            }
        }

        if(isAnyFollowerMatchIndexUpdated) {
            if (votersConfigStateMachine.isJointConsensus()) {
                long[] sortedMatchIndexInOldConfig = finalFollowers.stream()
                        .filter(follower -> votersConfigStateMachine.getConfigOld().contains(follower.getUri()))
                        .mapToLong(Follower::getMatchIndex)
                        .sorted().toArray();
                long nInOldConfig = sortedMatchIndexInOldConfig.length > 0 ?
                        sortedMatchIndexInOldConfig[sortedMatchIndexInOldConfig.length / 2] : journal.maxIndex();

                long[] sortedMatchIndexInNewConfig = finalFollowers.stream()
                        .filter(follower -> votersConfigStateMachine.getConfigNew().contains(follower.getUri()))
                        .mapToLong(Follower::getMatchIndex)
                        .sorted().toArray();
                long nInNewConfig = sortedMatchIndexInNewConfig.length > 0 ?
                        sortedMatchIndexInNewConfig[sortedMatchIndexInNewConfig.length / 2] : journal.maxIndex();

                N = Math.min(nInNewConfig, nInOldConfig);

            } else {
                long[] sortedMatchIndex = finalFollowers.stream()
                        .mapToLong(Follower::getMatchIndex)
                        .sorted().toArray();
                if (sortedMatchIndex.length > 0) {
                    N = sortedMatchIndex[sortedMatchIndex.length / 2];
                }
            }

        }
        if (voterState() == VoterState.LEADER && N > journal.commitIndex() && journal.getTerm(N - 1) == currentTerm.get()) {
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
        threads.wakeupThread(LEADER_APPEND_ENTRY_THREAD);
        threads.wakeupThread(LEADER_REPLICATION_THREAD);
    }

    /**
     * 处理follower中所有AppendEntriesRPC的响应
     * @param follower follower
     * @return 是否更新了follower的matchIndex
     */
    private boolean leaderHandleAppendEntriesResponse(Follower follower) {
        boolean isMatchIndexUpdated = false;
        AsyncAppendEntriesResponse response;
        while (serverState() == ServerState.RUNNING &&
                voterState() == VoterState.LEADER &&
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
            int finalTerm = currentTerm.get();
            if (finalTerm == response.getTerm()) {
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
                        int rollbackSize = (int) Math.min(config.getReplicationBatchSize(), follower.repStartIndex - journal.minIndex());
                        follower.repStartIndex -= rollbackSize;
                        sendAppendEntriesRequest(follower,
                                new AsyncAppendEntriesRequest(finalTerm, leader,
                                        follower.repStartIndex - 1,
                                        journal.getTerm(follower.repStartIndex - 1),
                                        journal.readRaw(follower.repStartIndex, rollbackSize),
                                        journal.commitIndex()));
                    }
                    delaySendAsyncAppendEntriesRpc(follower, new AsyncAppendEntriesRequest(finalTerm, leader,
                            response.getJournalIndex() - 1,
                            journal.getTerm(response.getJournalIndex() - 1),
                            journal.readRaw(response.getJournalIndex(), response.getEntryCount()),
                            journal.commitIndex()));
                }
            }
        }
        return isMatchIndexUpdated;
    }

    /**
     * 1. 如果 term < currentTerm返回 false
     * 如果 term > currentTerm且节点当前的状态不是FOLLOWER，将节点当前的状态转换为FOLLOWER；
     * 如果在prevLogIndex处的日志的任期号与prevLogTerm不匹配时，返回 false
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目
     * 如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个
     */
    private void followerHandleAppendEntriesRequest() throws InterruptedException, IOException {

        ReplicationRequestResponse rr = pendingAppendEntriesRequests.take();
        AsyncAppendEntriesRequest request = rr.getRequest();

        try {

            try {
                if (rr.getPrevLogIndex() < journal.minIndex() || journal.getTerm(rr.getPrevLogIndex()) == request.getPrevLogTerm()) {

                    try {
                        // TODO: 如果删除的entries中包括变更配置，需要回滚配置
                        final long startIndex = request.getPrevLogIndex() + 1;

                        maybeRollbackConfig(startIndex);

                        journal.compareOrAppendRaw(request.getEntries(), startIndex);

                        maybeUpdateConfig(request.getEntries());

                        AsyncAppendEntriesResponse response = new AsyncAppendEntriesResponse(true, rr.getPrevLogIndex() + 1,
                                currentTerm.get(), request.getEntries().size());
                        rr.getResponseFuture()
                                .complete(response);
                        if (request.getEntries() != null && !request.getEntries().isEmpty()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Send appendEntriesResponse, success: {}, " +
                                                "journalIndex: {}, entryCount: {}, term: {}, {}.",
                                        response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                                        voterInfo());
                            }
                        }
                    } catch (Throwable t) {
                        logger.warn("Handle replication request exception! {}", voterInfo(), t);
                        rr.getResponseFuture().complete(new AsyncAppendEntriesResponse(t));
                    } finally {
                        if (request.getLeaderCommit() > journal.commitIndex()) {
                            journal.commit(Math.min(request.getLeaderCommit(), journal.maxIndex()));
                            threads.wakeupThread(STATE_MACHINE_THREAD);
                            threads.wakeupThread(LEADER_APPEND_ENTRY_THREAD);
                        }
                    }
                    return;

                }
            } catch (IndexOverflowException | IndexUnderflowException ignored) {
            }

            AsyncAppendEntriesResponse response = new AsyncAppendEntriesResponse(false, rr.getPrevLogIndex() + 1,
                    currentTerm.get(), request.getEntries().size());
            rr.getResponseFuture()
                    .complete(response);
            if (request.getEntries() != null && !request.getEntries().isEmpty()) {

                if (logger.isDebugEnabled()) {
                    logger.debug("Send appendEntriesResponse, success: {}, journalIndex: {}, entryCount: {}, term: {}, " +
                                    "{}.",
                            response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                            voterInfo());
                }
            }
        } catch (Throwable t) {

            logger.warn("Exception when handle AsyncReplicationRequest, " +
                            "term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, entries: {}, leaderCommits: {}, " +
                            "{}.",
                    request.getTerm(), request.getLeader(), request.getPrevLogIndex(),
                    request.getPrevLogTerm(), request.getEntries().size(),
                    request.getLeaderCommit(), voterInfo(), t);
            throw t;
        }

    }

    private void maybeRollbackConfig(long startIndex) throws Exception {
        if(startIndex >= journal.maxIndex()) {
            return;
        }
        long index = journal.maxIndex(RESERVED_PARTITION);
        long startOffset = journal.readOffset(startIndex);
        while (--index >= journal.minIndex(RESERVED_PARTITION)) {
            RaftEntry entry = journal.readByPartition(RESERVED_PARTITION, index);
            if (entry.getHeader().getOffset() < startOffset) {
                break;
            }
            int reservedEntryType = ReservedEntriesSerializeSupport.parseEntryType(entry.getEntry());
            if(reservedEntryType == ReservedEntry.TYPE_UPDATE_VOTERS_S2) {
                UpdateVotersS2Entry updateVotersS2Entry = ReservedEntriesSerializeSupport.parse(entry.getEntry());
                votersConfigStateMachine.rollbackToJointConsensus(updateVotersS2Entry.getConfigOld());
            } else if(reservedEntryType == ReservedEntry.TYPE_UPDATE_VOTERS_S1) {
                votersConfigStateMachine.rollbackToOldConfig();
            }
        }
    }


    private void convertToCandidate() {
        synchronized (voterState) {
            voterState.convertToCandidate();
            resetAppendEntriesRpcMetricMap();
            this.followers.clear();

            currentTerm.incrementAndGet();
            votedFor = uri;
            electionTimeoutMs = randomInterval(config.electionTimeoutMs);
            logger.info("Convert to CANDIDATE, electionTimeout: {}, {}.", electionTimeoutMs, voterInfo());
        }
    }

    private void resetAppendEntriesRpcMetricMap() {
        appendEntriesRpcMetricMap.forEach((followerUri, metric) -> {
            removeMetric(getMetricName(METRIC_APPEND_ENTRIES_RPC, followerUri));
        });
        appendEntriesRpcMetricMap.clear();
    }

    private static String getMetricName(String prefix, URI followerUri) {
        return prefix + "_" + followerUri.toString();
    }

    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        synchronized (voterState) {
            voterState.convertToLeader();
            resetAppendEntriesRpcMetricMap();

            // 初始化followers
            this.followers.addAll(this.votersConfigStateMachine.voters().stream()
                    .filter(uri -> !uri.equals(this.uri))
                    .map(uri -> new Follower(uri, journal.maxIndex(), config.getReplicationParallelism()))
                    .collect(Collectors.toList()));

            this.followers.forEach(follower ->
                    appendEntriesRpcMetricMap.put(
                            follower.getUri(),
                            getMetric(getMetricName(METRIC_APPEND_ENTRIES_RPC, follower.getUri()))));
            // 变更状态
            journal.append(new Entry(
                    ReservedEntriesSerializeSupport.serialize(new LeaderAnnouncementEntry()),
                    currentTerm.get(), RESERVED_PARTITION));
            this.leader = this.uri;
            // Leader announcement
            threads.wakeupThread(LEADER_REPLICATION_THREAD);
            fireOnLeaderChangeEvent();

            logger.info("Convert to LEADER, {}.", voterInfo());
        }

    }

    @Override
    protected void onJournalRecovered(Journal journal) {
        super.onJournalRecovered(journal);
        maybeUpdateTermOnRecovery(journal);
    }

    private void maybeUpdateTermOnRecovery(Journal journal) {
        if(journal.minIndex() < journal.maxIndex()) {
            RaftEntry lastEntry = journal.read(journal.maxIndex() - 1);
            if(((EntryHeader) lastEntry.getHeader()).getTerm() > currentTerm.get()) {
                currentTerm.set(((EntryHeader) lastEntry.getHeader()).getTerm());
                logger.info("Set current term to {}, this is the term of the last entry in the journal.",
                        currentTerm.get());
            }
        }
    }

    private void fireOnLeaderChangeEvent() {
        Map<String, String> eventData = new HashMap<>();
        eventData.put("leader", this.serverUri().toString());
        eventData.put("term", String.valueOf(currentTerm.get()));
        fireEvent(EventType.ON_LEADER_CHANGE, eventData);
    }

    private void convertToFollower() {
        synchronized (voterState) {
            voterState.convertToFollower();
            resetAppendEntriesRpcMetricMap();
            this.votedFor = null;
            this.followers.clear();
            this.electionTimeoutMs = randomInterval(config.getElectionTimeoutMs());
            this.lastHeartbeat = System.currentTimeMillis();
            logger.info("Convert to FOLLOWER, electionTimeout: {}, {}.", electionTimeoutMs, voterInfo());
        }
    }


    @Override
    public Roll roll() {
        return Roll.VOTER;
    }

    /**
     * 将请求放到待处理队列中。
     */
    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {

        checkTerm(request.getTerm());

        if(request.getTerm() < currentTerm.get()) {
            // 如果收到的请求term小于当前term，拒绝请求
            return CompletableFuture.supplyAsync(() -> new AsyncAppendEntriesResponse(false, request.getPrevLogIndex() + 1,
                    currentTerm.get(), request.getEntries().size()));

        }

        if (voterState() != VoterState.FOLLOWER) {
            convertToFollower();
        }
        if (logger.isDebugEnabled() && request.getEntries() != null && !request.getEntries().isEmpty()) {
            logger.debug("Received appendEntriesRequest, term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                            "entries: {}, leaderCommit: {}, {}.",
                    request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                    request.getEntries().size(), request.getLeaderCommit(), voterInfo());
        }

        // reset heartbeat
        lastHeartbeat = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("Update lastHeartbeat, {}.", voterInfo());
        }

        if (!request.getLeader().equals(leader)) {
            leader = request.getLeader();
        }

        if (request.getEntries().size() > 0) {

            // 复制请求异步处理
            ReplicationRequestResponse requestResponse = new ReplicationRequestResponse(request);
            pendingAppendEntriesRequests.add(requestResponse);
            return requestResponse.getResponseFuture();
        } else {
            // 心跳直接返回成功
            return CompletableFuture.supplyAsync(() -> new AsyncAppendEntriesResponse(true, request.getPrevLogIndex() + 1,
                    currentTerm.get(), request.getEntries().size()));
        }
    }

    /**
     * 接收者收到requestVote方法后的实现流程如下：
     * <p>
     * 如果请求中的任期号 < 节点当前任期号，返回false；
     * 如果votedFor为空或者与candidateId相同，并且候选人的日志至少和自己的日志一样新，则给该候选人投票；
     */
    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("RequestVoteRpc received: term: {}, candidate: {}, " +
                            "lastLogIndex: {}, lastLogTerm: {}, {}.",
                    request.getTerm(), request.getCandidate(),
                    request.getLastLogIndex(), request.getLastLogTerm(), voterInfo());

            // 如果当前是LEADER或者上次收到心跳的至今小于最小选举超时，那直接拒绝投票
            if(voterState() == VoterState.LEADER || System.currentTimeMillis() - lastHeartbeat < config.getElectionTimeoutMs()) {
                return new RequestVoteResponse(request.getTerm(), false);
            }

            checkTerm(request.getTerm());

            synchronized (voteRequestMutex) {
                if (request.getTerm() < currentTerm.get()) {
                    return new RequestVoteResponse(request.getTerm(), false);
                }
                int lastLogTerm;
                if ((votedFor == null || votedFor.equals(request.getCandidate())) // If votedFor is null or candidateId
                        && (request.getLastLogTerm() > (lastLogTerm = journal.getTerm(journal.maxIndex() - 1))
                        || (request.getLastLogTerm() == lastLogTerm
                        && request.getLastLogIndex() >= journal.maxIndex() - 1)) // candidate’s log is at least as up-to-date as receiver’s log
                ) {
                    votedFor = request.getCandidate();
                    return new RequestVoteResponse(request.getTerm(), true);
                }
                return new RequestVoteResponse(request.getTerm(), false);
            }
        }, asyncExecutor);
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {

        UpdateStateRequestResponse requestResponse = new UpdateStateRequestResponse(request, updateClusterStateMetric);

        try {
            pendingUpdateStateRequests.put(requestResponse);
            if (request.getResponseConfig() == ResponseConfig.RECEIVE) {
                requestResponse.getResponseFuture().complete(new UpdateClusterStateResponse());
            }
        } catch (Throwable e) {
            requestResponse.getResponseFuture().complete(new UpdateClusterStateResponse(e));
        }
        return requestResponse.getResponseFuture();
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return waitLeadership()
                .thenCompose(aVoid -> state.query(querySerializer.parse(request.getQuery())))
                .thenApply(resultSerializer::serialize)
                .thenApply(QueryStateResponse::new)
                .exceptionally(exception -> {
                    try {
                        throw exception;
                    } catch (NotLeaderException e) {
                        return new QueryStateResponse(new NotLeaderException(leader));
                    } catch (Throwable t) {
                        return new QueryStateResponse(t);
                    }
                });
    }

    private void checkTerm(int term) {
        synchronized (currentTerm) {
            if (term > currentTerm.get()) {
                currentTerm.set(term);
                convertToFollower();
            }
        }
    }

    /**
     * 异步检测Leader有效性，成功返回null，失败抛出异常。
     */
    private CompletableFuture<Void> waitLeadership() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                if (voterState() == VoterState.LEADER) {
                    long start = System.currentTimeMillis();
                    while (!checkLeadership()) {

                        if (System.currentTimeMillis() - start > getRpcTimeoutMs()) {
                            throw new TimeoutException();
                        }
                        Thread.sleep(config.getHeartbeatIntervalMs() / 10);

                    }
                    completableFuture.complete(null);
                } else {
                    throw new NotLeaderException(leader);
                }
            } catch (InterruptedException e) {
                completableFuture.completeExceptionally(e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                completableFuture.completeExceptionally(e);
            }
        }, asyncExecutor);
        return completableFuture;
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return waitLeadership()
                .thenCompose(aVoid -> CompletableFuture.supplyAsync(() -> new LastAppliedResponse(state.lastApplied())))
                .exceptionally(exception -> {
                    try {
                        throw exception;
                    } catch (NotLeaderException e) {
                        return new LastAppliedResponse(new NotLeaderException(leader));
                    } catch (Throwable t) {
                        return new LastAppliedResponse(t);
                    }
                });
    }

    /**
     *
     * 在处理变更集群配置时，JournalKeeper采用RAFT协议中推荐的，二阶段变更的方式来避免在配置变更过程中可能出现的集群分裂。
     * 包括LEADER在内的，变更前后包含的所有节点，都通过二个阶段来安全的完成配置变更。配置变更期间，集群依然可以对外提供服务。
     *
     * * **共同一致阶段：** 每个节点在写入配置变更日志$C_{old, new}$后，不用等到这条日志被提交，立即变更配置，进入共同一致阶段。
     * 在这个阶段，使用新旧配置的两个集群（这个时候这两个集群可能共享大部分节点，并且拥有相同的LEADER节点）同时在线，每一条日志都需要，
     * 在使用新旧配置的二个集群中达成大多数一致。或者说，日志需要在新旧二个集群中，分别复制到超过半数以上的节点上，才能被提交。
     *
     * * **新配置阶段：** 每个节点在写入配置变更日志$C_{new}$后，不用等到这条日志被提交，立即变更配置，进入新配置阶段，完成配置变更。
     *
     * 当客户端调用updateVoters方法时:
     *
     * 1. LEADER先在本地写入配置变更日志$C_{old, new}$，然后立刻变更自身的配置为$C_{old, new}$，进入共同一致阶段。
     *
     * 2. 在共同一致阶段，LEADER把包括配置变更日志$C_{old, new}$和其它在这一阶段的其它日志，按照顺序一并复制到新旧两个集群的所有节点，
     * 每一条日志都需要在新旧二个集群都达到半数以上，才会被提交。**$C_{old, new}$被提交后，
     * 无论后续发生什么情况，这次配置变更最终都会执行成功。**
     *
     * 3. 处于新旧配置中的每个FOLLOWER节点，在收到并写入配置变更日志$C_{old, new}$后，无需等待日志提交，
     * 立刻将配置变更为$C_{old, new}$，并进入共同一致阶段。
     *
     * 4. LEADER在配置变更日志$C_{old, new}$提交后，写入新的配置变更日志$C_{new}$，然后立即变更自身的配置为$C_{new}$，进入新配置阶段。
     *
     * 5. 处于共同一致阶段中的每个FOLLOWER节点，在收到并写入配置变更日志$C_{new}$后，无需等待日志提交，立刻将配置变更为$C_{new}$，
     * 并进新配置阶段。此时节点需要检查一下自身是否还是是集群中的一员，如果不是，说明当前节点已经被从集群中移除，需要停止当前节点服务。
     *
     * 6. LEADER在$C_{new}$被提交后，也需要检查一下自身是否还是是集群中的一员，如果不是，说明当前节点已经被从集群中移除，
     * 需要停止当前节点服务。新集群会自动选举出新的LEADER。
     *
     * 如果变更过程中，节点发生了故障。为了确保节点能从故障中正确的恢复，需要保证：
     * **节点当前的配置总是和节点当前日志中最后一条配置变更日志（注意，这条日志可能已经提交也可能未被提交）保持一致。**
     *
     * 由于每个节点都遵循“写入配置变更日志-更新节点配置-提交配置变更日志”这样一个时序，所以，如果最后一条配置变更日志经被提交，
     * 那节点的配置和日志一定是一致的。但是，对于未提交配置变更日志，节点的配置有可能还没来得及更新就，节点宕机了。
     * 这种情况下，节点的配置是落后于日志的，因此，需要：
     *
     * * 在节点启动时进行检查，如果存在一条未提交的配置变更日志，如果节点配置和日志不一致，需要按照日志更新节点配置。
     * * 当节点删除未提交的日志时，如果被删除的日志中包含配置变更，需要将当前节点的配置也一并回滚；
     *
     * 在这个方法中，只是构造第一阶段的配置变更日志$C_{old, new}$，调用{@link #updateClusterState(UpdateClusterStateRequest)}方法，
     * 正常写入$C_{old, new}$，$C_{old, new}$被提交之后，会返回 {@link UpdateClusterStateResponse}，只要响应成功，
     * 虽然这时集群的配置并没有完成变更，但无论后续发生什么情况，集群最终都会完成此次变更。因此，直接返回客户端变更成功。
     *
     * @param request See {@link UpdateVotersRequest}
     * @return See {@link UpdateVotersResponse}
     */

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return CompletableFuture.supplyAsync(
                () -> new UpdateVotersS1Entry(request.getOldConfig(), request.getNewConfig()), asyncExecutor)
                .thenApply(ReservedEntriesSerializeSupport::serialize)
                .thenApply(entry -> new UpdateClusterStateRequest(entry, RESERVED_PARTITION, 1))
                .thenCompose(this::updateClusterState)
                .thenAccept(response -> {
                    if(!response.success()) {
                        throw new CompletionException(new UpdateConfigurationException("Failed to update voters configuration in step 1. " + response.errorString()));
                    }
                })
                .thenApply(aVoid -> new UpdateVotersResponse())
                .exceptionally(UpdateVotersResponse::new);
    }


    public VoterState voterState() {
        return voterState.getState();
    }

    @Override
    public void doStart() {
        convertToFollower();
        this.checkElectionTimeoutFuture = scheduledExecutor.scheduleAtFixedRate(this::checkElectionTimeout,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);

    }

    @Override
    public void doStop() {
        try {
            stopAndWaitScheduledFeature(checkElectionTimeoutFuture, 1000L);
            if (followers != null) {
                followers.forEach(follower -> follower.pendingResponses.clear());
            }
            pendingAppendEntriesRequests.clear();
            pendingUpdateStateRequests.clear();

        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception, {}: ", voterInfo(), t);
        }

    }

    @Override
    protected void beforeStateChanged(ER updateResult) {
        super.beforeStateChanged(updateResult);
        if(null != updateResult) {
            replicationCallbacks.setResult(state.lastApplied(), updateResult);
        }
    }

    @Override
    protected void afterStateChanged(ER result) {
        super.afterStateChanged(result);
        threads.wakeupThread(LEADER_CALLBACK_THREAD);
    }

    @Override
    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = super.createServerMetadata();
        serverMetadata.setCurrentTerm(currentTerm.get());
        serverMetadata.setVotedFor(votedFor);
        return serverMetadata;
    }

    @Override
    protected void onMetadataRecovered(ServerMetadata metadata) {
        super.onMetadataRecovered(metadata);
        this.currentTerm.set(metadata.getCurrentTerm());
        this.votedFor = metadata.getVotedFor();

    }

    private String voterInfo() {
        return String.format("voterState: %s, currentTerm: %d, minIndex: %d, " +
                        "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                voterState.getState(), currentTerm.get(), journal.minIndex(),
                journal.maxIndex(), journal.commitIndex(), state.lastApplied(), uri.toString());
    }

    public enum VoterState {LEADER, FOLLOWER, CANDIDATE}


    private static class VoterStateMachine {
        private VoterState state = VoterState.FOLLOWER;

        private void convertToLeader() {
            if(state == VoterState.CANDIDATE) {
                state = VoterState.LEADER;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.LEADER));
            }
        }

        private void convertToFollower() {
            state = VoterState.FOLLOWER;
        }

        private void convertToCandidate() {
            if(state == VoterState.CANDIDATE || state == VoterState.FOLLOWER) {
                state = VoterState.CANDIDATE;
            } else {
                throw new IllegalStateException(String.format("Change voter state from %s to %s is not allowed!", state, VoterState.FOLLOWER));
            }
        }

        public VoterState getState() {
            return state;
        }
    }

    private static class ReplicationRequestResponse {
        private final AsyncAppendEntriesRequest request;
        private final CompletableFuture<AsyncAppendEntriesResponse> responseFuture;


        ReplicationRequestResponse(AsyncAppendEntriesRequest request) {
            this.request = request;
            responseFuture = new CompletableFuture<>();
        }

        AsyncAppendEntriesRequest getRequest() {
            return request;
        }

        CompletableFuture<AsyncAppendEntriesResponse> getResponseFuture() {
            return responseFuture;
        }

        int getPrevLogTerm() {
            return request.getPrevLogTerm();
        }

        long getPrevLogIndex() {
            return request.getPrevLogIndex();
        }
    }

    private static class UpdateStateRequestResponse {
        private final UpdateClusterStateRequest request;
        private final CompletableFuture<UpdateClusterStateResponse> responseFuture;
        private final long start = System.nanoTime();
        private long logIndex;

        UpdateStateRequestResponse(UpdateClusterStateRequest request, JMetric metric) {
            this.request = request;
            responseFuture = new CompletableFuture<>();

            responseFuture
                    .thenRun(() -> metric.mark(System.nanoTime() - start, request.getEntry().length));

        }

        UpdateClusterStateRequest getRequest() {
            return request;
        }

        CompletableFuture<UpdateClusterStateResponse> getResponseFuture() {
            return responseFuture;
        }

        public long getLogIndex() {
            return logIndex;
        }

        public void setLogIndex(long logIndex) {
            this.logIndex = logIndex;
        }
    }


    private static class Follower {


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

        private Follower(URI uri, long nextIndex, int replicationParallelism) {
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

    public static class Config extends Server.Config {
        public final static long DEFAULT_HEARTBEAT_INTERVAL_MS = 100L;
        public final static long DEFAULT_ELECTION_TIMEOUT_MS = 500L;
        public final static int DEFAULT_REPLICATION_BATCH_SIZE = 128;
        public final static int DEFAULT_REPLICATION_PARALLELISM = 16;
        public final static int DEFAULT_CACHE_REQUESTS = 1024;

        public final static String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval_ms";
        public final static String ELECTION_TIMEOUT_KEY = "election_timeout_ms";
        public final static String REPLICATION_BATCH_SIZE_KEY = "replication_batch_size";
        public final static String REPLICATION_PARALLELISM_KEY = "replication_parallelism";
        public final static String CACHE_REQUESTS_KEY = "cache_requests";

        private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        private long electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;  // 最小选举超时
        private int replicationBatchSize = DEFAULT_REPLICATION_BATCH_SIZE;
        private int replicationParallelism = DEFAULT_REPLICATION_PARALLELISM;
        private int cacheRequests = DEFAULT_CACHE_REQUESTS;

        public int getReplicationBatchSize() {
            return replicationBatchSize;
        }

        public void setReplicationBatchSize(int replicationBatchSize) {
            this.replicationBatchSize = replicationBatchSize;
        }


        public long getHeartbeatIntervalMs() {
            return heartbeatIntervalMs;
        }

        public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public long getElectionTimeoutMs() {
            return electionTimeoutMs;
        }

        public void setElectionTimeoutMs(long electionTimeoutMs) {
            this.electionTimeoutMs = electionTimeoutMs;
        }

        public int getReplicationParallelism() {
            return replicationParallelism;
        }

        public void setReplicationParallelism(int replicationParallelism) {
            this.replicationParallelism = replicationParallelism;
        }

        public int getCacheRequests() {
            return cacheRequests;
        }

        public void setCacheRequests(int cacheRequests) {
            this.cacheRequests = cacheRequests;
        }

    }


}
