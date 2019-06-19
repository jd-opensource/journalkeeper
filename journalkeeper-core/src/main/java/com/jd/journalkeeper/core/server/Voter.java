package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.core.entry.reserved.LeaderAnnouncementEntry;
import com.jd.journalkeeper.core.entry.reserved.LeaderAnnouncementEntrySerializer;
import com.jd.journalkeeper.exceptions.ServerBusyException;
import com.jd.journalkeeper.utils.event.EventType;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.entry.Entry;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.persistence.ServerMetadata;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.server.*;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.jd.journalkeeper.core.api.RaftJournal.RESERVED_PARTITION;


/**
 * @author liyue25
 * Date: 2019-03-18
 */
public class Voter<E, Q, R> extends Server<E, Q, R> {
    private static final Logger logger = LoggerFactory.getLogger(Voter.class);
    /**
     * 选民状态，在LEADER、FOLLOWER和CANDIDATE之间转换。初始值为FOLLOWER。
     */
    private VoterState voterState = VoterState.FOLLOWER;
    /**
     * Voter最后知道的任期号（从 0 开始递增）
     */
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    /**
     * 在当前任期内收到选票的候选人地址（如果没有就为 null）
     */
    private URI votedFor = null;

    /**
     * 已获得的选票
     */
    private AtomicInteger grantedVotes = new AtomicInteger(0);

    /**
     * 串行处理所有RequestVoterRPC request/response
     */
    private final Object voteRequestMutex = new Object();
    private final Object voteResponseMutex = new Object();
    /**
     * 串行处理所有角色变更
     */
    private final Object rollMutex = new Object();
    /**
     * 选举（心跳）超时
     */
    private long electionTimeoutMs;
    // LEADER ONLY
    /**
     * 当角色为LEADER时，记录所有FOLLOWER的位置等信息
     */
    private List<Follower> followers = new ArrayList<>();

    private final BlockingQueue<UpdateStateRequestResponse> pendingUpdateStateRequests;

    /**
     * 上次从LEADER收到心跳（asyncAppendEntries）的时间戳
     */
    private long lastHeartbeat;

    /**
     * 待处理的asyncAppendEntries Request，按照request中的preLogTerm和prevLogIndex排序。
     */
    private BlockingQueue<ReplicationRequestResponse> pendingAppendEntriesRequests;

    private final Config config;
    /**
     * Leader接收客户端请求串行写入entries线程
     */
    private final LoopThread leaderAppendJournalEntryThread;
    /**
     * Voter 处理AppendEntriesRequest线程
     */
    private final LoopThread voterReplicationRequestHandlerThread;
    /**
     * Voter 处理AppendEntriesRequest线程
     */
    private final LoopThread callbackThread;

    /**
     * Leader 发送AppendEntries RPC线程
     */
    private final LoopThread leaderReplicationThread;

    /**
     * Leader 处理AppendEntries RPC Response 线程
     */
    private final LoopThread leaderReplicationResponseHandlerThread;

    private ScheduledFuture checkElectionTimeoutFuture;

    private final LeaderAnnouncementEntrySerializer leaderAnnouncementEntrySerializer = new LeaderAnnouncementEntrySerializer();

    private final CallbackPositioningBelt replicationCallbacks = new CallbackPositioningBelt();
    private final CallbackPositioningBelt flushCallbacks = new CallbackPositioningBelt();

    /**
     * 刷盘位置
     */
    private final AtomicLong journalFlushIndex = new AtomicLong(0L);

    public Voter(StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, Serializer<Q> querySerializer, Serializer<R> resultSerializer, ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {
        super(stateFactory, entrySerializer, querySerializer, resultSerializer, scheduledExecutor, asyncExecutor, properties);
        this.config = toConfig(properties);
        electionTimeoutMs = randomInterval(config.getElectionTimeoutMs());

        pendingUpdateStateRequests = new ArrayBlockingQueue<>(config.getCacheRequests());
        pendingAppendEntriesRequests = new PriorityBlockingQueue<>(config.getCacheRequests(),
                Comparator.comparing(ReplicationRequestResponse::getPrevLogTerm)
                        .thenComparing(ReplicationRequestResponse::getPrevLogIndex));

        leaderAppendJournalEntryThread = buildLeaderAppendJournalEntryThread();
        voterReplicationRequestHandlerThread = buildVoterReplicationHandlerThread();
        leaderReplicationThread = buildLeaderReplicationThread();
        leaderReplicationResponseHandlerThread = buildLeaderReplicationResponseThread();
        callbackThread = buildCallbackThread();
    }

    private LoopThread buildLeaderAppendJournalEntryThread() {
        return LoopThread.builder()
                .name("LeaderAppendJournalEntryThread")
                .condition(() ->this.serverState() == ServerState.RUNNING
                        && journal.maxIndex() - journalFlushIndex.get() < config.getOnFlyEntries()
                        && journal.maxIndex() - commitIndex < config.getOnFlyEntries())
                .doWork(this::appendJournalEntry)
                .sleepTime(0,0)
                .onException(e -> logger.warn("LeaderAppendJournalEntry Exception, {}: ", voterInfo(), e))
                .daemon(true)
                .build();
    }

    private LoopThread buildVoterReplicationHandlerThread() {
        return LoopThread.builder()
                .name("VoterReplicationRequestHandlerThread")
                .condition(() ->this.serverState() == ServerState.RUNNING)
                .doWork(this::handleReplicationRequest)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("VoterReplicationHandlerThread Exception, {}: ", voterInfo(), e))
                .daemon(true)
                .build();
    }

    private LoopThread buildLeaderReplicationThread() {
        return LoopThread.builder()
                .name("LeaderReplicationThread")
                .condition(() ->this.serverState() == ServerState.RUNNING)
                .doWork(this::replication)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("LeaderReplicationThread Exception, {}: ", voterInfo(), e))
                .daemon(true)
                .build();
    }
    private LoopThread buildLeaderReplicationResponseThread() {
        return LoopThread.builder()
                .name("LeaderReplicationResponseThread")
                .condition(() ->this.serverState() == ServerState.RUNNING && this.voterState == VoterState.LEADER)
                .doWork(this::handleReplicationResponses)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("LeaderReplicationResponseThread Exception, {}: ", voterInfo(), e))
                .daemon(true)
                .build();
    }
    private LoopThread buildCallbackThread() {
        return LoopThread.builder()
                .name("CallbackThread")
                .condition(() ->this.serverState() == ServerState.RUNNING)
                .doWork(this::callback)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("CallbackThread Exception, {}: ", voterInfo(), e))
                .daemon(true)
                .build();
    }

    private void callback() {
        replicationCallbacks.callbackBefore(state.lastApplied());
        flushCallbacks.callbackBefore(journalFlushIndex.get());
    }

    private final AtomicInteger appendJournalCounter = new AtomicInteger(0);
    /**
     * 串行写入日志
     */
    private void appendJournalEntry() throws InterruptedException {
        if(journal.maxIndex() - journalFlushIndex.get() < config.getOnFlyEntries()
            && journal.maxIndex() - commitIndex < config.getOnFlyEntries()) {
            UpdateStateRequestResponse rr = pendingUpdateStateRequests.take();
            if (voterState == VoterState.LEADER) {
                try {
                    appendJournalCounter.incrementAndGet();
                    long index = journal.append(new Entry(rr.getRequest().getEntry(), currentTerm.get(), rr.getRequest().getPartition(), rr.getRequest().getBatchSize()));
                    if (rr.getRequest().getResponseConfig() == ResponseConfig.PERSISTENCE) {
                        flushCallbacks.put(new Callback(index, rr.getResponseFuture()));
                    } else if (rr.getRequest().getResponseConfig() == ResponseConfig.REPLICATION) {
                        replicationCallbacks.put(new Callback(index, rr.getResponseFuture()));
                    }
                    logger.debug("Append journal entry, {}", voterInfo());
                    // 唤醒复制线程
                    leaderReplicationThread.wakeup();
                } catch (Throwable t) {
                    rr.getResponseFuture().complete(new UpdateClusterStateResponse(t));
                    throw t;
                }
            } else {
                logger.warn("NOT_LEADER!");
                rr.getResponseFuture().complete(new UpdateClusterStateResponse(new NotLeaderException(leader)));
            }
        }
    }

    @Override
    public boolean flush() {
        boolean ret = super.flush();
        journalFlushIndex.set(journal.maxIndex());
        leaderAppendJournalEntryThread.wakeup();
        callbackThread.wakeup();
        return ret;
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
        config.setOnFlyEntries(Integer.parseInt(
                properties.getProperty(
                        Config.ON_FLY_ENTRIES_KEY,
                        String.valueOf(Config.DEFAULT_ON_FLY_ENTRIES))));
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
        return followers.stream()
                .map(Follower::getLastHeartbeatResponseTime)
                .filter(lastHeartbeat -> now - lastHeartbeat < 2 * config.getHeartbeatIntervalMs())
                .count() >= voters.size() / 2;
    }

    private void checkElectionTimeout() {
        try {
            if (voterState != VoterState.LEADER && System.currentTimeMillis() - lastHeartbeat > electionTimeoutMs) {
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
     *  4.1. 如果收到了来自大多数服务器的投票：成为LEADER
     *  4.2. 如果收到了来自新领导人的asyncAppendEntries请求（heartbeat）：转换状态为FOLLOWER
     *  4.3. 如果选举超时：开始新一轮的选举
     */
    private void startElection() {
        logger.info("Start election, {}", voterInfo());
        convertToCandidate();

        lastHeartbeat = System.currentTimeMillis();
        long lastLogIndex = journal.maxIndex() - 1 ;
        int lastLogTerm = journal.getTerm(lastLogIndex);

        // 处理单节点的特殊情况
        synchronized (voteResponseMutex) {
            if (grantedVotes.get() >= voters.size() / 2 + 1) {
                convertToLeader();
            }
        }


        RequestVoteRequest request = new RequestVoteRequest(currentTerm.get(), uri, lastLogIndex, lastLogTerm);

        voters.parallelStream()
                .filter(uri -> !uri.equals(this.uri))
                .map(uri -> {
                    try {
                        return getServerRpc(uri);
                    } catch (Throwable t) {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .forEach(serverRpc ->
                        serverRpc.requestVote(request)
                .exceptionally(RequestVoteResponse::new)
                .thenAccept(response -> {
                    if(response.success()) {
                        checkTerm(response.getTerm());
                        synchronized (voteResponseMutex) {
                            if (response.getTerm() == currentTerm.get() && voterState == VoterState.CANDIDATE && response.isVoteGranted()) {
                                grantedVotes.incrementAndGet();
                                logger.info("{} votes granted, {}.", grantedVotes.get(), voterInfo());
                                if (grantedVotes.get() >= voters.size() / 2 + 1) {
                                    convertToLeader();
                                }
                            }
                        }
                    }
                }));
    }

    /**
     * 反复检查每个FOLLOWER的下一条复制位置nextIndex和本地日志log[]的最大位置，
     * 如果存在差异，发送asyncAppendEntries请求，同时更新对应FOLLOWER的nextIndex。
     * 复制发送线程只负责发送asyncAppendEntries请求，不处理响应。
     */
    private void replication() {
        int count;
        // 如果是单节点，直接唤醒leaderReplicationResponseHandlerThread，减少响应时延。
        if(serverState() == ServerState.RUNNING && voterState == VoterState.LEADER && followers.isEmpty()) {
            leaderReplicationResponseHandlerThread.wakeup();
        }
        do {
            if (serverState() == ServerState.RUNNING && voterState == VoterState.LEADER && !Thread.currentThread().isInterrupted()) {

                count = followers.stream()
                        .filter(follower -> follower.nextIndex - follower.repStartIndex <= config.getOnFlyEntries())
                        .mapToInt(follower -> {
                            long maxIndex = journal.maxIndex();
                            int rpcCount = 0;
                            if (follower.nextIndex < maxIndex) {
                                List<byte []> entries = journal.readRaw(follower.getNextIndex(), config.getReplicationBatchSize());
                                AsyncAppendEntriesRequest request =
                                        new AsyncAppendEntriesRequest(currentTerm.get(), leader,
                                                follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                                entries, commitIndex);
                                sendAsyncAppendEntriesRpc(follower, request);
                                follower.setNextIndex(follower.getNextIndex() + entries.size());
                                rpcCount++;

                            } else {
                                // Send heartbeat
                                if(System.currentTimeMillis() - follower.getLastHeartbeatRequestTime() >= config.getHeartbeatIntervalMs()) {
                                    AsyncAppendEntriesRequest request =
                                            new AsyncAppendEntriesRequest(currentTerm.get(), leader,
                                                    follower.getNextIndex() - 1, getPreLogTerm(follower.getNextIndex()),
                                                    Collections.emptyList(), commitIndex);
                                    sendAsyncAppendEntriesRpc(follower, request);
                                }
                            }
                            return rpcCount;
                        }).sum();

            } else {
                count = 0;
            }
        } while (count > 0);

    }


    private int getPreLogTerm(long currentLogIndex) {
        if(currentLogIndex > journal.minIndex()) {
            return journal.getTerm(currentLogIndex - 1);
        } else if (currentLogIndex == journal.maxIndex() && snapshots.containsKey(currentLogIndex)) {
            return snapshots.get(currentLogIndex).lastIncludedTerm();
        } else if (currentLogIndex == 0 ) {
            return  -1;
        } else {
            throw new IndexUnderflowException();
        }
    }

    private void sendAsyncAppendEntriesRpc(Follower follower, AsyncAppendEntriesRequest request) {
        if(serverState() == ServerState.RUNNING && voterState == VoterState.LEADER && request.getTerm() == currentTerm.get()) {
            follower.setLastHeartbeatRequestTime(System.currentTimeMillis());
            CompletableFuture.supplyAsync(() -> {
                ServerRpc serverRpc = getServerRpc(follower.getUri());
                if (null != request.getEntries() && !request.getEntries().isEmpty()) {
                    logger.debug("Send appendEntriesRequest, " +
                                    "follower: {}, " +
                                    "term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                                    "entries: {}, leaderCommit: {}, {}.",
                            follower.getUri(),
                            request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                            request.getEntries().size(), request.getLeaderCommit(), voterInfo());
                } else {
                    logger.debug("Send heartbeat, term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                                    "entries: {}, leaderCommit: {}, {}.",
                            request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                            request.getEntries().size(), request.getLeaderCommit(), voterInfo());
                }
                return serverRpc;
            }, asyncExecutor)
                    .thenCompose(serverRpc -> serverRpc.asyncAppendEntries(request))
                    .exceptionally(AsyncAppendEntriesResponse::new)
                    .thenAccept(response -> {
                        if (response.success()) {
                            logger.debug("Update lastHeartbeatResponseTime of {}, {}.", follower.getUri(), voterInfo());
                            follower.setLastHeartbeatResponseTime(System.currentTimeMillis());

                            checkTerm(response.getTerm());
                            if(response.getTerm() == currentTerm.get()) {
                                if(response.getEntryCount() > 0) {
                                    logger.debug("Handle appendEntriesResponse, success: {}, journalIndex: {}, " +
                                                    "entryCount: {}, term: {}, follower: {}, {}.",
                                            response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                                            follower.getUri(), voterInfo());
                                    follower.addResponse(response);
                                    leaderReplicationResponseHandlerThread.wakeup();
                                } else {
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

                });

        } else {
            logger.warn("Drop AsyncAppendEntries Request: follower: {}, term: {}, " +
                            "prevLogIndex: {}, prevLogTerm: {}, entries: {}, " +
                            "leader: {}, leaderCommit: {}, {}.",
                    follower.getUri(), request.getTerm(), request.getPrevLogIndex(),
                    request.getPrevLogTerm(), request.getEntries().size(),
                    request.getLeader(), request.getLeaderCommit(),
                    voterInfo());
        }

    }

    private void delaySendAsyncAppendEntriesRpc(Follower follower, AsyncAppendEntriesRequest request) {
        new Timer("Retry-AsyncAppendEntriesRpc", true).schedule(new TimerTask() {
            @Override
            public void run() {
                sendAsyncAppendEntriesRpc(follower, request);
            }
        }, config.getHeartbeatIntervalMs());
    }


    /**
     * 对于每一个AsyncAppendRequest RPC请求，当收到成功响应的时需要更新repStartIndex、matchIndex和commitIndex。
     * 由于接收者按照日志的索引位置串行处理请求，一般情况下，收到的响应也是按照顺序返回的，但是考虑到网络延时和数据重传，
     * 依然不可避免乱序响应的情况。LEADER在处理响应时需要遵循：
     *
     * 1. 对于所有响应，先比较返回值中的term是否与当前term一致，如果不一致说明任期已经变更，丢弃响应，
     * 2. LEADER 反复重试所有term一致的超时和失败请求（考虑到性能问题，可以在每次重试前加一个时延）；
     * 3. 对于返回失败的请求，如果这个请求是所有在途请求中日志位置最小的（repStartIndex == logIndex），
     *    说明接收者的日志落后于repStartIndex，这时LEADER需要回退，再次发送AsyncAppendRequest RPC请求，
     *    直到找到FOLLOWER与LEADER相同的位置。
     * 4. 对于成功的响应，需要按照日志索引位置顺序处理。规定只有返回值中的logIndex与repStartIndex相等时，
     *    才更新repStartIndex和matchIndex，否则反复重试直到满足条件；
     * 5. 如果存在一个索引位置N，这个N是所有满足如下所有条件位置中的最大值，则将commitIndex更新为N。
     *  5.1 超过半数的matchIndex都大于等于N
     *  5.2 N > commitIndex
     *  5.3 log[N].term == currentTerm
     */
    private void handleReplicationResponses() {
        if (followers.isEmpty()) { // 单节点情况需要单独处理
            long N = journal.maxIndex();
            if (voterState == VoterState.LEADER && N > commitIndex && journal.getTerm(N - 1) == currentTerm.get()) {
                commitIndex = N;
                onCommitted();

            }
        } else {
            long[] sortedMatchIndex = followers.parallelStream()
                    .peek(this::handleReplicationResponse)
                    .mapToLong(Follower::getMatchIndex)
                    .sorted().toArray();
            if (sortedMatchIndex.length > 0) {
                long N = sortedMatchIndex[sortedMatchIndex.length / 2];
                if (voterState == VoterState.LEADER && N > commitIndex && journal.getTerm(N - 1) == currentTerm.get()) {
                    logger.debug("Set commitIndex {} to {}, {}.",
                            commitIndex, N, voterInfo());
                    commitIndex = N;
                    onCommitted();
                }
            }
        }
    }

    private void onCommitted() {
        // 唤醒状态机线程
        stateMachineThread.wakeup();
        leaderAppendJournalEntryThread.wakeup();
        leaderReplicationThread.wakeup();
    }

    private void handleReplicationResponse(Follower follower) {
        AsyncAppendEntriesResponse response;
        while (serverState() == ServerState.RUNNING && (response = follower.pendingResponses.poll()) != null) {
            if(response.getEntryCount() > 0) {
                logger.debug("Received appendEntriesResponse, success: {}, journalIndex: {}, " +
                                "entryCount: {}, term: {}, follower: {}, {}.",
                        response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                        follower.getUri(), voterInfo());
            } else {
                logger.debug("Received heartbeat response, success: {}, journalIndex: {}, " +
                                "entryCount: {}, term: {}, follower: {}, {}.",
                        response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                        follower.getUri(), voterInfo());
            }
            int fixTerm = currentTerm.get();
            if(fixTerm == response.getTerm()) {
                if(response.isSuccess()) {

                    if (follower.getRepStartIndex() == response.getJournalIndex()) {
                        follower.setRepStartIndex(follower.getRepStartIndex() + response.getEntryCount());
                        follower.setMatchIndex(response.getJournalIndex() + response.getEntryCount());
                        if(response.getEntryCount() > 0) {
                            logger.debug("Replication success, RepStartIndex: {}, matchIndex: {}, follower: {}, {}.",
                                    follower.getRepStartIndex(), follower.getMatchIndex(), follower.getUri(),
                                    voterInfo());
                        }
                    } else {
                        // 出现空洞，重新放回队列，等待后续处理
                        follower.addResponse(response);
                        break;
                    }
                } else if(response.getEntryCount() > 0){
                    // 失败且不是心跳
                    if (follower.getRepStartIndex() == response.getJournalIndex()) {
                        // 需要回退
                        int rollbackSize = (int) Math.min(config.getReplicationBatchSize(), follower.repStartIndex - journal.minIndex());
                        follower.repStartIndex -= rollbackSize;
                        sendAsyncAppendEntriesRpc(follower,
                                new AsyncAppendEntriesRequest(fixTerm, leader,
                                        follower.repStartIndex - 1,
                                        journal.getTerm(follower.repStartIndex - 1),
                                        journal.readRaw(follower.repStartIndex, rollbackSize),
                                        commitIndex));
                    }
                    delaySendAsyncAppendEntriesRpc(follower, new AsyncAppendEntriesRequest(fixTerm, leader,
                            response.getJournalIndex() - 1,
                            journal.getTerm(response.getJournalIndex() - 1),
                            journal.readRaw(response.getJournalIndex(), response.getEntryCount()),
                            commitIndex));
                }
            }
        }
    }

    /**
     * 1. 如果 term < currentTerm返回 false
     * 如果 term > currentTerm且节点当前的状态不是FOLLOWER，将节点当前的状态转换为FOLLOWER；
     * 如果在prevLogIndex处的日志的任期号与prevLogTerm不匹配时，返回 false
     * 如果一条已经存在的日志与新的冲突（index 相同但是任期号 term 不同），则删除已经存在的日志和它之后所有的日志
     * 添加任何在已有的日志中不存在的条目
     * 如果leaderCommit > commitIndex，将commitIndex设置为leaderCommit和最新日志条目索引号中较小的一个
     */
    private void handleReplicationRequest() throws InterruptedException {

        ReplicationRequestResponse rr = pendingAppendEntriesRequests.take();
        AsyncAppendEntriesRequest request = rr.getRequest();

        try {

            try {
                if (rr.getPrevLogIndex() < journal.minIndex() || journal.getTerm(rr.getPrevLogIndex()) == request.getPrevLogTerm()) {

                    try {
                        journal.compareOrAppendRaw(request.getEntries(), request.getPrevLogIndex() + 1);

                        AsyncAppendEntriesResponse response = new AsyncAppendEntriesResponse(true, rr.getPrevLogIndex() + 1,
                                currentTerm.get(), request.getEntries().size());
                        rr.getResponseFuture()
                                .complete(response);
                        if (request.getEntries() != null && !request.getEntries().isEmpty()) {
                            logger.debug("Send appendEntriesResponse, success: {}, " +
                                            "journalIndex: {}, entryCount: {}, term: {}, {}.",
                                    response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                                    voterInfo());
                        }
                    } catch (Throwable t) {
                        logger.warn("Handle replication request exception! {}", voterInfo(), t);
                        rr.getResponseFuture().complete(new AsyncAppendEntriesResponse(t));
                    } finally {
                        if (request.getLeaderCommit() > commitIndex) {
                            commitIndex = Math.min(request.getLeaderCommit(), journal.maxIndex());
                            stateMachineThread.wakeup();
                            leaderAppendJournalEntryThread.wakeup();

                        }
                    }
                    return;

                }
            } catch (IndexOverflowException | IndexUnderflowException ignored) {}

            AsyncAppendEntriesResponse response = new AsyncAppendEntriesResponse(false, rr.getPrevLogIndex() + 1,
                    currentTerm.get(), request.getEntries().size());
            rr.getResponseFuture()
                    .complete(response);
            if(request.getEntries() != null && !request.getEntries().isEmpty()) {
                logger.debug("Send appendEntriesResponse, success: {}, journalIndex: {}, entryCount: {}, term: {}, " +
                                "{}.",
                        response.isSuccess(), response.getJournalIndex(), response.getEntryCount(), response.getTerm(),
                        voterInfo());
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


    private void convertToCandidate() {
        synchronized (rollMutex) {
            this.followers = Collections.emptyList();
            this.voterState = VoterState.CANDIDATE;
            currentTerm.incrementAndGet();
            votedFor = uri;
            grantedVotes.set(1);
            electionTimeoutMs = randomInterval(config.electionTimeoutMs);
            logger.info("Convert to CANDIDATE, electionTimeout: {}, {}.", electionTimeoutMs, voterInfo());
        }
    }

    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        synchronized (rollMutex) {

            // 初始化followers
            this.followers = this.voters.stream()
                    .filter(uri -> !uri.equals(this.uri))
                    .map(uri -> new Follower(uri, journal.maxIndex(), config.getReplicationParallelism()))
                    .collect(Collectors.toList());
            // 变更状态
            journal.append(new Entry(
                    leaderAnnouncementEntrySerializer.serialize(new LeaderAnnouncementEntry()),
                    currentTerm.get(), RESERVED_PARTITION));
            this.voterState = VoterState.LEADER;
            this.leader = this.uri;
            // Leader announcement
            leaderReplicationThread.wakeup();
            fireOnLeaderChangeEvent();
            logger.info("Convert to LEADER, {}.", voterInfo());
        }

    }

    private void fireOnLeaderChangeEvent() {
        Map<String, String> eventData = new HashMap<>();
        eventData.put("leader", this.serverUri().toString());
        eventData.put("term", String.valueOf(currentTerm.get()));
        fireEvent(EventType.ON_LEADER_CHANGE, eventData);
    }

    private void convertToFollower() {
        synchronized (rollMutex) {
            this.votedFor = null;
            this.followers = Collections.emptyList();
            this.electionTimeoutMs = randomInterval(config.getElectionTimeoutMs());
            this.lastHeartbeat = System.currentTimeMillis();
            this.voterState = VoterState.FOLLOWER;
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
        if(voterState != VoterState.FOLLOWER) {
            convertToFollower();
        }
        if(request.getEntries() != null && !request.getEntries().isEmpty()) {
            logger.debug("Received appendEntriesRequest, term: {}, leader: {}, prevLogIndex: {}, prevLogTerm: {}, " +
                            "entries: {}, leaderCommit: {}, {}.",
                    request.getTerm(), request.getLeader(), request.getPrevLogIndex(), request.getPrevLogTerm(),
                    request.getEntries().size(), request.getLeaderCommit(), voterInfo());
        }

        // reset heartbeat
        lastHeartbeat = System.currentTimeMillis();
        logger.debug("Update lastHeartbeat, {}.", voterInfo());


        if(!request.getLeader().equals(leader)) {
            leader = request.getLeader();
        }

        if(request.getEntries().size() > 0) {

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
     *
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
            checkTerm(request.getTerm());

            synchronized (voteRequestMutex) {
                if (request.getTerm() < currentTerm.get()) {
                    return new RequestVoteResponse(request.getTerm(), false);
                }
                int lastLogTerm;
                if((votedFor == null || votedFor.equals(request.getCandidate())) // If votedFor is null or candidateId
                        && (request.getLastLogTerm() >  (lastLogTerm = journal.getTerm(journal.maxIndex() -1))
                            || (request.getLastLogTerm() == lastLogTerm
                            && request.getLastLogIndex() >= journal.maxIndex() -1)) // candidate’s log is at least as up-to-date as receiver’s log
                        ) {
                    votedFor = request.getCandidate();
                    return new RequestVoteResponse(request.getTerm(), true);
                }
                return new RequestVoteResponse(request.getTerm(), false);
            }
        }, asyncExecutor);
    }

    private final AtomicInteger updateCounter = new AtomicInteger(0);
    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        UpdateStateRequestResponse requestResponse = new UpdateStateRequestResponse(request);

        try {
            pendingUpdateStateRequests.add(requestResponse);
            updateCounter.incrementAndGet();

            leaderAppendJournalEntryThread.wakeup();
            if (request.getResponseConfig() == ResponseConfig.RECEIVE) {
                requestResponse.getResponseFuture().complete(new UpdateClusterStateResponse());
            }
        } catch (IllegalStateException ie) {
            requestResponse.getResponseFuture().complete(new UpdateClusterStateResponse(new ServerBusyException()));
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
            if(term > currentTerm.get()) {
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
                if(voterState == VoterState.LEADER) {
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

    @Override
    public void doStart() {
        convertToFollower();
        this.checkElectionTimeoutFuture = scheduledExecutor.scheduleAtFixedRate(this::checkElectionTimeout,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        leaderAppendJournalEntryThread.start();
        voterReplicationRequestHandlerThread.start();
        leaderReplicationThread.start();
        leaderReplicationResponseHandlerThread.start();
        callbackThread.start();

        LoopThread.builder()
                .name("MonitorThread")
                .condition(() ->this.voterState == VoterState.LEADER)
                .doWork(() -> logger.info(voterInfo()))
                .sleepTime(1000,1000)
                .daemon(true)
                .build().start();
    }

    @Override
    public void doStop() {
        try {
            stopAndWaitScheduledFeature(checkElectionTimeoutFuture, 1000L);
            stopLeaderReplicationResponseHandler();
            leaderReplicationThread.stop();
            stopVoterReplicationRequestHandler();
            stopLeaderUpdateStateRequestHandler();
            callbackThread.stop();

        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception, {}: ", voterInfo(), t);
        }

    }

    private void stopLeaderUpdateStateRequestHandler() {
        leaderAppendJournalEntryThread.stop();
        pendingUpdateStateRequests.clear();
    }

    private void stopVoterReplicationRequestHandler() {
        voterReplicationRequestHandlerThread.stop();
        pendingAppendEntriesRequests.clear();
    }

    private void stopLeaderReplicationResponseHandler() {
        logger.info("StopLeaderReplicationResponseHandler, {}.", voterInfo());
        leaderReplicationResponseHandlerThread.stop();
        if(followers!= null) {
            followers.forEach(follower -> follower.pendingResponses.clear());
        }
    }

    @Override
    protected void onStateChanged() {
        super.onStateChanged();
        callbackThread.wakeup();
    }

    @Override
    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = super.createServerMetadata();
        serverMetadata.setCurrentTerm(currentTerm.get());
        serverMetadata.setVotedFor(votedFor);
        serverMetadata.setVoters(voters);
        return serverMetadata;
    }

    @Override
    protected void onMetadataRecovered(ServerMetadata metadata) {
        super.onMetadataRecovered(metadata);
        this.currentTerm.set(metadata.getCurrentTerm());
        this.votedFor = metadata.getVotedFor();
        this.voters = metadata.getVoters();

    }

    private String voterInfo() {
        return String.format("updateCounter: %d, appendJournalCounter: %d, voterState: %s, currentTerm: %d, minIndex: %d, " +
                "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                updateCounter.get(), appendJournalCounter.get(),
                voterState.toString(), currentTerm.get(), journal.minIndex(),
                journal.maxIndex(), commitIndex, state.lastApplied(), uri.toString());
    }

    enum VoterState {LEADER, FOLLOWER, CANDIDATE}


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

        private long logIndex;

        UpdateStateRequestResponse(UpdateClusterStateRequest request) {
            this.request = request;
            responseFuture = new CompletableFuture<>();
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
        /**
         * 仅LEADER使用，待处理的asyncAppendEntries Response，按照Response中的logIndex排序。
         */
        private final BlockingQueue<AsyncAppendEntriesResponse> pendingResponses;
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
    private static class Callback {
        final long position;
        final long timestamp;
        final CompletableFuture<UpdateClusterStateResponse> completableFuture;

        public Callback(long position, CompletableFuture<UpdateClusterStateResponse> completableFuture) {
            this.position = position;
            this.timestamp = System.currentTimeMillis();
            this.completableFuture = completableFuture;
        }
    }

    private class CallbackPositioningBelt {

        private final ConcurrentLinkedQueue<Callback> queue = new ConcurrentLinkedQueue<>();
        private AtomicLong callbackPosition = new AtomicLong(0L);
        Callback getFirst() {
            final Callback f = queue.peek();
            if (f == null)
                throw new NoSuchElementException();
            return f;
        }
        Callback removeFirst() {
            final Callback f = queue.poll();
            if (f == null)
                throw new NoSuchElementException();
            return f;
        }

        boolean remove(Callback callback) { return queue.remove(callback);}
        void addLast(Callback callback) {
            queue.add(callback);
        }

        private AtomicLong counter = new AtomicLong(0L);
        private AtomicLong putCounter = new AtomicLong(0L);
        /**
         * NOT Thread-safe!!!!!!
         */
        void callbackBefore(long position) {
            callbackPosition.set(position);
            try {
                while (getFirst().position <= position){
                    Callback callback = removeFirst();
                    counter.incrementAndGet();
                    callback.completableFuture.complete(new UpdateClusterStateResponse());
                }
                long deadline = System.currentTimeMillis() - getRpcTimeoutMs();
                while (getFirst().timestamp < deadline) {
                    Callback callback = removeFirst();
                    counter.incrementAndGet();
                    callback.completableFuture.complete(new UpdateClusterStateResponse(new TimeoutException()));
                }
            } catch (NoSuchElementException ignored) {}
        }

        void put(Callback callback) {
            putCounter.incrementAndGet();
            addLast(callback);
            if(callback.position <= callbackPosition.get() && remove(callback)){
                counter.incrementAndGet();
                callback.completableFuture.complete(new UpdateClusterStateResponse());
            }
        }
    }

    public static class Config extends Server.Config {
        public final static long DEFAULT_HEARTBEAT_INTERVAL_MS = 50L;
        public final static long DEFAULT_ELECTION_TIMEOUT_MS = 1000L;
        public final static int DEFAULT_REPLICATION_BATCH_SIZE = 128;
        public final static int DEFAULT_REPLICATION_PARALLELISM = 16;
        public final static int DEFAULT_CACHE_REQUESTS = 1024;
        public final static int DEFAULT_ON_FLY_ENTRIES = 1024;

        public final static String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval_ms";
        public final static String ELECTION_TIMEOUT_KEY = "election_timeout_ms";
        public final static String REPLICATION_BATCH_SIZE_KEY = "replication_batch_size";
        public final static String REPLICATION_PARALLELISM_KEY = "replication_parallelism";
        public final static String CACHE_REQUESTS_KEY = "cache_requests";
        public final static String ON_FLY_ENTRIES_KEY = "on_fly_entries";

        private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        private long electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;
        private int replicationBatchSize = DEFAULT_REPLICATION_BATCH_SIZE;
        private int replicationParallelism = DEFAULT_REPLICATION_PARALLELISM;
        private int cacheRequests = DEFAULT_CACHE_REQUESTS;
        private int onFlyEntries = DEFAULT_ON_FLY_ENTRIES;
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

        public int getOnFlyEntries() {
            return onFlyEntries;
        }

        public void setOnFlyEntries(int onFlyEntries) {
            this.onFlyEntries = onFlyEntries;
        }
    }
}
