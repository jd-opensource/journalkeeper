package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.api.StorageEntry;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


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
    private int currentTerm = 0;
    /**
     * 在当前任期内收到选票的候选人地址（如果没有就为 null）
     */
    private URI votedFor = null;
    /**
     * 选举（心跳）超时
     */
    private long electionTimeoutMs = 0L;
    /**
     * 心跳间隔
     */
    private long heartbeatIntervalMs = 0L;
    // LEADER ONLY
    /**
     * 当角色为LEADER时，记录所有FOLLOWER的位置等信息
     */
    private List<Follower> followers = new ArrayList<>();

    private final BlockingQueue<UpdateStateRequestResponse<E>> pendingUpdateRequests;

    /**
     * 上次从LEADER收到心跳（asyncAppendEntries）的时间戳
     */
    private long lastHeartbeat;

    /**
     * 待处理的asyncAppendEntries Request，按照request中的preLogTerm和prevLogIndex排序。
     */
    private BlockingQueue<ReplicationRequestResponse<E>> pendingAppendRequests;

    private final Config config;
    /**
     * Leader接收客户端请求串行写入entries线程
     */
    private final LoopThread leaderAppendJournalEntryThread;
    /**
     * Voter 处理AppendEntriesRequest线程
     */
    private final LoopThread voterReplicationHandlerThread;

    /**
     * Leader 发送AppendEntries RPC线程
     */
    private final LoopThread leaderReplicationThread;

    /**
     * Leader 处理AppendEntries RPC Response 线程
     */
    private final LoopThread leaderReplicationResponseThread;

    private ScheduledFuture checkElectionTimeoutFuture;

    private final CallbackPositioningBelt callbackPositioningBelt = new CallbackPositioningBelt();

    public Voter(StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {
        super(stateFactory, entrySerializer, scheduledExecutor, asyncExecutor, properties);
        this.config = toConfig(properties);
        electionTimeoutMs = randomInterval(config.getElectionTimeoutMs());

        pendingUpdateRequests = new ArrayBlockingQueue<>(config.getCacheRequests());
        pendingAppendRequests = new PriorityBlockingQueue<>(config.getCacheRequests(),
                Comparator.comparing(ReplicationRequestResponse<E>::getPrevLogTerm)
                        .thenComparing(ReplicationRequestResponse::getPrevLogIndex));

        leaderAppendJournalEntryThread = buildLeaderAppendJournalEntryThread();
        voterReplicationHandlerThread = buildVoterReplicationHandlerThread();
        leaderReplicationThread = buildLeaderReplicationThread();
        leaderReplicationResponseThread = buildLeaderReplicationResponseThread();
    }

    private LoopThread buildLeaderAppendJournalEntryThread() {
        return LoopThread.builder()
                .name("LeaderAppendJournalEntryThread")
                .doWork(this::appendJournalEntry)
                .sleepTime(0,0)
                .onException(e -> logger.warn("LeaderAppendJournalEntry Exception: ", e))
                .build();
    }

    private LoopThread buildVoterReplicationHandlerThread() {
        return LoopThread.builder()
                .name("ReplicationHandlerThread")
                .doWork(this::handleReplicationRequest)
                .sleepTime(0,0)
                .onException(e -> logger.warn("VoterReplicationHandlerThread Exception: ", e))
                .build();
    }

    private LoopThread buildLeaderReplicationThread() {
        return LoopThread.builder()
                .name("LeaderReplicationThread")
                .doWork(this::replication)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("LeaderReplicationThread Exception: ", e))
                .build();
    }
    private LoopThread buildLeaderReplicationResponseThread() {
        return LoopThread.builder()
                .name("LeaderReplicationResponseThread")
                .doWork(this::handleReplicationResponses)
                .sleepTime(config.getHeartbeatIntervalMs(),config.getHeartbeatIntervalMs())
                .onException(e -> logger.warn("LeaderReplicationResponseThread Exception: ", e))
                .build();
    }

    /**
     * 串行写入日志
     */
    private void appendJournalEntry() throws InterruptedException {
        UpdateStateRequestResponse<E> rr = pendingUpdateRequests.take();
        if(voterState == VoterState.LEADER) {
            long index = journal.append(new StorageEntry<>(rr.request.getEntry(), currentTerm));
            callbackPositioningBelt.put(new Callback(index, rr.getResponseFuture()));
            // 唤醒复制线程
            leaderReplicationThread.weakup();
        } else {
            rr.getResponseFuture().complete(new UpdateClusterStateResponse(new NotLeaderException(leader)));
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
        return config;
    }

    private void sendHeartbeat(Follower follower) {
        if(System.currentTimeMillis() - follower.lastHeartbeat > heartbeatIntervalMs) {
            ServerRpc<E, Q, R> serverRpc = getServerRpc(follower.uri);
            long prevIndex = follower.nextIndex - 1;
            assert serverRpc != null;
            CompletableFuture<AsyncAppendEntriesResponse> future = serverRpc.asyncAppendEntries(
                    new AsyncAppendEntriesRequest<>(currentTerm, leader, prevIndex,
                            journal.getTerm(prevIndex),null, commitIndex )
            );
            future.exceptionally(exception -> {
                logger.warn("Heartbeat Exception on {}, : ", follower.getUri(), exception);
                return null;
            });
            future.thenAccept(asyncAppendEntriesResponse -> {
                if(asyncAppendEntriesResponse.getException() != null) {
                    follower.setLastHeartbeat(System.currentTimeMillis());
                } else {
                    logger.warn("Heartbeat Exception on {}, : ", follower.getUri(), asyncAppendEntriesResponse.getException());
                }
            });
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
        long now = System.currentTimeMillis();
        return followers.stream()
                .map(Follower::getLastHeartbeat)
                .filter(lastHeartbeat -> now - lastHeartbeat < 2 * heartbeatIntervalMs)
                .count() >= voters.size() / 2;
    }

    private void checkElectionTimeout() {
        if(voterState != VoterState.LEADER && System.currentTimeMillis() - lastHeartbeat > electionTimeoutMs) {
            startElection();
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
        convertToCandidate();
        currentTerm += 1;
        votedFor = uri;
        lastHeartbeat = System.currentTimeMillis();
        electionTimeoutMs = randomInterval(config.electionTimeoutMs);
        long lastLogIndex = journal.maxIndex() - 1;
        RequestVoteRequest request = new RequestVoteRequest(currentTerm, uri, lastLogIndex, journal.getTerm(lastLogIndex));

        int grantedVotes = 1; // 自己投给自己一票
        grantedVotes += voters.parallelStream()
                .filter(uri -> !uri.equals(this.uri))
                .map(this::getServerRpc).filter(Objects::nonNull)
                .map(serverRpc -> serverRpc.requestVote(request))
                .map(future -> {
                    try {
                        RequestVoteResponse response = future.get(electionTimeoutMs, TimeUnit.MILLISECONDS);
                        return response.getTerm() == currentTerm && response.isVoteGranted();
                    } catch (Throwable e) {
                        logger.warn("Request vote exception: ", e);
                    }
                    return false;
                }).filter(voteGranted -> voteGranted).count();


        if(grantedVotes > voters.size() / 2 + 1) {
            convertToLeader();
        }
    }

    /**
     * 反复检查每个FOLLOWER的下一条复制位置nextIndex和本地日志log[]的最大位置，
     * 如果存在差异，发送asyncAppendEntries请求，同时更新对应FOLLOWER的nextIndex。
     * 复制发送线程只负责发送asyncAppendEntries请求，不处理响应。
     */
    private void replication() {
        int count;
        do {
            if (voterState == VoterState.LEADER) {
                count = followers.parallelStream()
                        .mapToInt(follower -> {
                            long maxIndex = journal.maxIndex();
                            int rpcCount = 0;
                            if (follower.nextIndex < maxIndex) {
                                long nextIndex = Math.max(maxIndex, follower.nextIndex + config.getReplicationBatchSize());
                                List<StorageEntry<E>> entries = journal.readRaw(follower.nextIndex, config.getReplicationBatchSize());
                                AsyncAppendEntriesRequest<StorageEntry<E>> request =
                                        new AsyncAppendEntriesRequest<>(currentTerm, leader,
                                                nextIndex - 1, journal.getTerm(nextIndex - 1),
                                                entries, commitIndex);
                                sendAsyncAppendEntriesRpc(follower, request);
                                follower.nextIndex += entries.size();
                                rpcCount++;
                            } else {
                                if(System.currentTimeMillis() - follower.lastHeartbeat >= config.getHeartbeatIntervalMs()) {
                                    sendHeartbeat(follower);
                                    rpcCount++;
                                }
                            }
                            return rpcCount;
                        }).sum();
            } else {
                count = 0;
            }
        } while (count > 0);
    }

    private void sendAsyncAppendEntriesRpc(Follower follower, AsyncAppendEntriesRequest<StorageEntry<E>> request) {
        CompletableFuture<AsyncAppendEntriesResponse> future;
        try {
            ServerRpc<E, Q, R> serverRpc = getServerRpc(follower.getUri());
            assert serverRpc != null;
            future = serverRpc.asyncAppendEntries(request);
        } catch (Throwable t) {
            // 将所有异常扔到响应里，在处理响应的线程中统一处理
            future = CompletableFuture.supplyAsync(() -> new AsyncAppendEntriesResponse(t));
        }

        future.whenComplete((response, exception) -> {
            if(request.getTerm() == currentTerm) {
                if(response != null && response.getException() != null) {
                    follower.addResponse(response);
                    leaderReplicationResponseThread.weakup();
                } else {

                    logger.warn("Replication exception: ", response == null ? exception: response.getException());
                    delaySendAsyncAppendEntriesRpc(follower, request);
                }
            } else {
                logger.warn("Drop outdated AsyncAppendEntries Request: currentTerm: {}, uri: {}, term: {}, index: {}.",
                        currentTerm, follower.getUri(), request.getTerm(), request.getPrevLogIndex());
            }
        });
    }

    private void delaySendAsyncAppendEntriesRpc(Follower follower, AsyncAppendEntriesRequest<StorageEntry<E>> request) {
        new Timer("Retry-AsyncAppendEntriesRpc", true).schedule(new TimerTask() {
            @Override
            public void run() {
                sendAsyncAppendEntriesRpc(follower, request);
            }
        }, heartbeatIntervalMs);
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
        long [] sortedMatchIndex = followers.parallelStream()
                .peek(this::handleReplicationResponse)
                .mapToLong(Follower::getMatchIndex)
                .sorted().toArray();

        long N = sortedMatchIndex[sortedMatchIndex.length / 2];
        if(voterState == VoterState.LEADER && N > commitIndex && journal.getTerm(N) == currentTerm) {
            commitIndex = N;
            // 唤醒状态机线程
            stateMachineThread.weakup();
        }
    }

    private void handleReplicationResponse(Follower follower) {
        Iterator<AsyncAppendEntriesResponse> iterator = follower.pendingResponses.iterator();
        int retry = 0;
        while (iterator.hasNext() && retry < config.getReplicationParallelism()){
            AsyncAppendEntriesResponse response = iterator.next();
            int fixTerm = currentTerm;
            if(fixTerm == response.getTerm()) {
                if (follower.getRepStartIndex() == response.getJournalIndex()) {
                    if (response.isSuccess()) {
                        follower.setRepStartIndex(follower.getRepStartIndex() + response.getEntryCount());
                        follower.setMatchIndex(response.getJournalIndex() + response.getEntryCount());
                        follower.setLastHeartbeat(System.currentTimeMillis());

                    } else {
                        int rollbackSize = (int ) Math.min(config.getReplicationBatchSize(), follower.repStartIndex - journal.minIndex());
                        follower.repStartIndex -= rollbackSize;
                        sendAsyncAppendEntriesRpc(follower,
                                new AsyncAppendEntriesRequest<>(fixTerm, leader,
                                        follower.repStartIndex - 1,
                                        journal.getTerm(follower.repStartIndex - 1),
                                        journal.readRaw(follower.repStartIndex,rollbackSize),
                                        commitIndex ));
                        delaySendAsyncAppendEntriesRpc(follower, new AsyncAppendEntriesRequest<>(fixTerm, leader,
                                response.getJournalIndex() - 1,
                                journal.getTerm(response.getJournalIndex() - 1),
                                journal.readRaw(response.getJournalIndex(),response.getEntryCount()),
                                commitIndex));
                        retry++;
                    }
                    follower.repStartIndex += response.getEntryCount();
                } else {
                    //noinspection StatementWithEmptyBody
                    if(response.isSuccess()) {
                        // 不可能走到这个分支
                    } else {
                        delaySendAsyncAppendEntriesRpc(follower, new AsyncAppendEntriesRequest<>(fixTerm, leader,
                                response.getJournalIndex() - 1,
                                journal.getTerm(response.getJournalIndex() - 1),
                                journal.readRaw(response.getJournalIndex(),response.getEntryCount()),
                                commitIndex));
                        retry++;
                    }
                }
            }
            iterator.remove();
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

        ReplicationRequestResponse<E> rr = pendingAppendRequests.take();
        if( rr.getRequest().getTerm() < currentTerm) {
            rr.getResponseFuture()
                    .complete(new AsyncAppendEntriesResponse(false, rr.getPrevLogIndex() + 1,
                            rr.getRequest().getTerm(), rr.getRequest().getEntries().size()));
            return;
        }
        if(rr.getRequest().getTerm() > currentTerm ) {
            currentTerm = rr.getRequest().getTerm();
            convertToFollower();
        }

        if( journal.getTerm(rr.getPrevLogIndex()) != rr.getRequest().getPrevLogIndex()) {
            rr.getResponseFuture()
                    .complete(new AsyncAppendEntriesResponse(false, rr.getPrevLogIndex() + 1,
                            rr.getRequest().getTerm(), rr.getRequest().getEntries().size()));
            return;
        }

        journal.compareOrAppend(rr.getRequest().getEntries(), rr.getRequest().getPrevLogIndex() + 1);
        if(rr.getRequest().getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(rr.getRequest().getLeaderCommit(), journal.maxIndex());
        }

        rr.getResponseFuture()
                .complete(new AsyncAppendEntriesResponse(true, rr.getPrevLogIndex() + 1,
                        rr.getRequest().getTerm(), rr.getRequest().getEntries().size()));

    }


    private void convertToCandidate() {

        this.followers.clear();
        this.voterState = VoterState.CANDIDATE;
        logger.info("Convert to CANDIDATE, currentTerm: {}.", currentTerm);
    }

    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        // 初始化followers
        this.followers = this.voters.stream()
                .filter(uri -> !uri.equals(this.uri))
                .map(uri -> new Follower(uri, journal.maxIndex()))
                .collect(Collectors.toList());
        // 变更状态
        this.voterState = VoterState.LEADER;
        // 发送心跳
        leaderReplicationThread.weakup();
        logger.info("Convert to LEADER, currentTerm: {}.", currentTerm);

    }

    private void convertToFollower() {
        this.followers.clear();
        this.electionTimeoutMs = randomInterval(config.getHeartbeatIntervalMs());
        this.lastHeartbeat = System.currentTimeMillis();
        this.voterState = VoterState.FOLLOWER;
        logger.info("Convert to FOLLOWER, currentTerm: {}.", currentTerm);

    }

    private ServerRpc<E, Q, R> getServerRpc(URI uri) {
        return null;
    }

    @Override
    public Roll roll() {
        return Roll.VOTER;
    }

    /**
     * 将请求放到待处理队列中。
     */
    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest<StorageEntry<E>> request) {
        ReplicationRequestResponse<E> requestResponse = new ReplicationRequestResponse<>(request);
        pendingAppendRequests.add(requestResponse);
        return requestResponse.getResponseFuture();
    }

    /**
     * 接收者收到requestVote方法后的实现流程如下：
     *
     * 如果请求中的任期号 < 节点当前任期号，返回false；
     * 如果votedFor为空或者与candidateId相同，并且候选人的日志和自己的日志一样新，则给该候选人投票；
     */
    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            synchronized (votedFor) {

                if (request.getTerm() < currentTerm) {
                    return new RequestVoteResponse(request.getTerm(), false);
                }
                if(request.getTerm() > currentTerm) {
                    currentTerm = request.getTerm();
                    convertToFollower();
                }
                if(votedFor == null || votedFor.equals(request.getCandidateAddr())) {
                    votedFor = request.getCandidateAddr();
                    return new RequestVoteResponse(request.getTerm(), true);
                }
                return new RequestVoteResponse(request.getTerm(), false);
            }
        });
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest<E> request) {
        UpdateStateRequestResponse<E> requestResponse = new UpdateStateRequestResponse<>(request);
        try {
            pendingUpdateRequests.put(requestResponse);
            return requestResponse.getResponseFuture();
        } catch (InterruptedException e) {
            logger.warn("Exception: ", e);
            return CompletableFuture.supplyAsync(() -> new UpdateClusterStateResponse(e));
        }
    }

    @Override
    public CompletableFuture<QueryStateResponse<R>> queryClusterState(QueryStateRequest<Q> request) {
        return waitLeadership()
                .thenCompose(aVoid -> state.query(request.getQuery()))
                .thenApply(QueryStateResponse::new)
                .exceptionally(exception -> {
                    try {
                        throw exception;
                    } catch (NotLeaderException e) {
                        return new QueryStateResponse<>(new NotLeaderException(leader));
                    } catch (Throwable t) {
                        return new QueryStateResponse<>(t);
                    }
                });
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
                        Thread.sleep(heartbeatIntervalMs / 10);

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
        });
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
    public void start() {
        convertToFollower();
        super.start();
        leaderAppendJournalEntryThread.start();
        voterReplicationHandlerThread.start();
        leaderReplicationThread.start();
        leaderReplicationResponseThread.start();
        this.checkElectionTimeoutFuture = scheduledExecutor.scheduleAtFixedRate(this::checkElectionTimeout,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        try {
            stopAndWaitScheduledFeature(checkElectionTimeoutFuture, 1000L);
            leaderReplicationResponseThread.stop();
            leaderReplicationThread.stop();
            voterReplicationHandlerThread.stop();
            leaderAppendJournalEntryThread.stop();

            super.stop();
        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception: ", t);
        }

    }

    @Override
    protected void onStateChanged() {
        super.onStateChanged();
        callbackPositioningBelt.callbackBefore(state.lastApplied());
    }

    @Override
    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = super.createServerMetadata();
        serverMetadata.setCurrentTerm(currentTerm);
        serverMetadata.setVotedFor(votedFor);
        serverMetadata.setVoters(voters);
        return serverMetadata;
    }

    @Override
    protected void onMetadataRecovered(ServerMetadata metadata) {
        super.onMetadataRecovered(metadata);
        this.currentTerm = metadata.getCurrentTerm();
        this.votedFor = metadata.getVotedFor();
        this.voters = metadata.getVoters();

    }

    enum VoterState {LEADER, FOLLOWER, CANDIDATE}


    private static class ReplicationRequestResponse<E> {
        private final AsyncAppendEntriesRequest<StorageEntry<E>> request;
        private final CompletableFuture<AsyncAppendEntriesResponse> responseFuture;


        ReplicationRequestResponse(AsyncAppendEntriesRequest<StorageEntry<E>> request) {
            this.request = request;
            responseFuture = new CompletableFuture<>();
        }

        AsyncAppendEntriesRequest<StorageEntry<E>> getRequest() {
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

    private static class UpdateStateRequestResponse<E> {
        private final UpdateClusterStateRequest<E> request;
        private final CompletableFuture<UpdateClusterStateResponse> responseFuture;

        private long logIndex;

        UpdateStateRequestResponse(UpdateClusterStateRequest<E> request) {
            this.request = request;
            responseFuture = new CompletableFuture<>();
        }

        UpdateClusterStateRequest<E> getRequest() {
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
        private long lastHeartbeat;

        /**
         * 仅LEADER使用，待处理的asyncAppendEntries Response，按照Response中的logIndex排序。
         */
        private SortedSet<AsyncAppendEntriesResponse> pendingResponses =
                Collections.synchronizedSortedSet(
                new TreeSet<>(Comparator.comparing(AsyncAppendEntriesResponse::getTerm)
                    .thenComparing(AsyncAppendEntriesResponse::getJournalIndex)));
        private Follower(URI uri, long nextIndex) {
            this.uri = uri;
            this.nextIndex = nextIndex;
            this.repStartIndex = nextIndex;
            this.lastHeartbeat = System.currentTimeMillis();
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

        long getLastHeartbeat() {
            return lastHeartbeat;
        }

        void setLastHeartbeat(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }

        void addResponse(AsyncAppendEntriesResponse response) {
            pendingResponses.add(response);
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
        /**
         * NOT Thread-safe!!!!!!
         */
        void callbackBefore(long position) {
            callbackPosition.set(position);
            try {
                while (getFirst().position <= position){
                    Callback callback = removeFirst();
                    callback.completableFuture.complete(new UpdateClusterStateResponse());
                }
                long deadline = System.currentTimeMillis() - getRpcTimeoutMs();
                while (getFirst().timestamp < deadline) {
                    Callback callback = removeFirst();
                    callback.completableFuture.complete(new UpdateClusterStateResponse(new TimeoutException()));
                }
            } catch (NoSuchElementException ignored) {}
        }

        void put(Callback callback) {
            addLast(callback);
            if(callback.position <= callbackPosition.get() && remove(callback)){
                callback.completableFuture.complete(new UpdateClusterStateResponse());
            }
        }
    }

    public static class Config extends Server.Config {
        public final static long DEFAULT_HEARTBEAT_INTERVAL_MS = 50L;
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
        private long electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;
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
