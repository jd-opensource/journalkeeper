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
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.SerializedUpdateRequest;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.SnapshotEntry;
import io.journalkeeper.core.api.SnapshotsEntry;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.core.entry.internal.InternalEntriesSerializeSupport;
import io.journalkeeper.core.entry.internal.InternalEntryType;
import io.journalkeeper.core.entry.internal.UpdateVotersS1Entry;
import io.journalkeeper.core.exception.UpdateConfigurationException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.core.state.ConfigState;
import io.journalkeeper.core.state.JournalKeeperState;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.client.CompleteTransactionRequest;
import io.journalkeeper.rpc.client.CompleteTransactionResponse;
import io.journalkeeper.rpc.client.CreateTransactionRequest;
import io.journalkeeper.rpc.client.CreateTransactionResponse;
import io.journalkeeper.rpc.client.GetOpeningTransactionsResponse;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.client.GetSnapshotsResponse;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.rpc.client.UpdateVotersResponse;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.DisableLeaderWriteRequest;
import io.journalkeeper.rpc.server.DisableLeaderWriteResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.INTERNAL_PARTITION;


/**
 * @author LiYue
 * Date: 2019-03-18
 */
class Voter<E, ER, Q, QR> extends AbstractServer<E, ER, Q, QR> implements CheckTermInterceptor{
    private static final Logger logger = LoggerFactory.getLogger(Voter.class);

    /**
     * 触发切换指定Leader的门限值
     */
    private static final long PREFERRED_LEADER_IN_SYNC_THRESHOLD = 128L;
    /**
     * Voter最后知道的任期号（从 0 开始递增）
     */
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    // LEADER ONLY
    /**
     * 串行处理所有RequestVoterRPC request/response
     */
    private final Object voteRequestMutex = new Object();

    private final Config config;



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
     * 上次从LEADER收到心跳（asyncAppendEntries）的时间戳
     */
    private long lastHeartbeat = 0L;

    /**
     * 检查选举超时定时任务
     */
    private ScheduledFuture checkElectionTimeoutFuture;
    private ScheduledFuture printStateFuture;

    private Leader<E, ER, Q, QR> leader;
    private Follower follower;

    Voter(StateFactory<E, ER, Q, QR> stateFactory, Serializer<E> entrySerializer, Serializer<ER> entryResultSerializer,
                 Serializer<Q> querySerializer, Serializer<QR> resultSerializer,
                 JournalEntryParser journalEntryParser,
                 ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, ServerRpcAccessPoint serverRpcAccessPoint, Properties properties) {
        super(stateFactory, entrySerializer, entryResultSerializer, querySerializer, resultSerializer,
                journalEntryParser, scheduledExecutor, asyncExecutor, serverRpcAccessPoint, properties);
        this.config = toConfig(properties);

        state.addInterceptor(InternalEntryType.TYPE_UPDATE_VOTERS_S1, this::applyUpdateVotersInternalEntry);
        state.addInterceptor(InternalEntryType.TYPE_UPDATE_VOTERS_S2, this::applyUpdateVotersInternalEntry);

        electionTimeoutMs =  config.getElectionTimeoutMs() + randomInterval(config.getElectionTimeoutMs());
    }


    private void applyUpdateVotersInternalEntry(InternalEntryType type, byte [] internalEntry) {
        voterConfigManager.applyReservedEntry(type, internalEntry, voterState(), state.getConfigState(),
                this, serverUri(), this);

    }

    @Override
    protected void onJournalFlushed() {
        if(null != leader) {
            leader.onJournalFlushed();
        }
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setElectionTimeoutMs(Long.parseLong(
                properties.getProperty(
                        Config.ELECTION_TIMEOUT_KEY,
                        String.valueOf(Config.DEFAULT_ELECTION_TIMEOUT_MS))));
        config.setSnapshotIntervalSec(Integer.parseInt(
                properties.getProperty(
                        Config.SNAPSHOT_INTERVAL_SEC_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_SNAPSHOT_INTERVAL_SEC))));
        config.setJournalRetentionMin(Integer.parseInt(
                properties.getProperty(
                        Config.JOURNAL_RETENTION_MIN_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_JOURNAL_RETENTION_MIN))));

        config.setHeartbeatIntervalMs(Long.parseLong(
                properties.getProperty(
                        Config.HEARTBEAT_INTERVAL_KEY,
                        String.valueOf(Config.DEFAULT_HEARTBEAT_INTERVAL_MS))));
        config.setTransactionTimeoutMs(Long.parseLong(
                properties.getProperty(
                        Config.TRANSACTION_TIMEOUT_MS_KEY,
                        String.valueOf(Config.DEFAULT_TRANSACTION_TIMEOUT_MS))));
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
        config.setRpcTimeoutMs(Long.parseLong(
                properties.getProperty(
                        AbstractServer.Config.RPC_TIMEOUT_MS_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_RPC_TIMEOUT_MS))));
        config.setFlushIntervalMs(Long.parseLong(
                properties.getProperty(
                        AbstractServer.Config.FLUSH_INTERVAL_MS_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_FLUSH_INTERVAL_MS))));
        config.setPrintStateIntervalSec(Integer.parseInt(
                properties.getProperty(
                        Config.PRINT_STATE_INTERVAL_SEC_KEY,
                        String.valueOf(Config.DEFAULT_PRINT_STATE_INTERVAL_SEC))));

        config.setWorkingDir(Paths.get(
                properties.getProperty(AbstractServer.Config.WORKING_DIR_KEY,
                        config.getWorkingDir().normalize().toString())));

        config.setGetStateBatchSize(Integer.parseInt(
                properties.getProperty(
                        AbstractServer.Config.GET_STATE_BATCH_SIZE_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_GET_STATE_BATCH_SIZE))));

        config.setEnableMetric(Boolean.parseBoolean(
                properties.getProperty(
                        AbstractServer.Config.ENABLE_METRIC_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_ENABLE_METRIC))));

        config.setPrintMetricIntervalSec(Integer.parseInt(
                properties.getProperty(
                        AbstractServer.Config.PRINT_METRIC_INTERVAL_SEC_KEY,
                        String.valueOf(AbstractServer.Config.DEFAULT_PRINT_METRIC_INTERVAL_SEC))));
        return config;
    }

    // 下次发起选举的时间
    private long nextElectionTime = 0L;

    private void checkElectionTimeout() {
        try {
            if (voterState() == VoterState.FOLLOWER && System.currentTimeMillis() - lastHeartbeat > electionTimeoutMs) {
                convertToCandidate();
                nextElectionTime = System.currentTimeMillis() + electionTimeoutMs;
            }

            if(voterState() == VoterState.CANDIDATE && System.currentTimeMillis() > nextElectionTime) {

                startElection(false);
            }

            if(checkPreferredLeader()) {
                convertToCandidate();
                startElection(true);
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
    private void startElection(boolean fromPreferredLeader) {

        nextElectionTime = Long.MAX_VALUE;
        votedFor = uri;
        currentTerm.incrementAndGet();
        logger.info("Start election, {}", voterInfo());

        long lastLogIndex = journal.maxIndex() - 1;
        int lastLogTerm = journal.getTerm(lastLogIndex);

        RequestVoteRequest request = new RequestVoteRequest(currentTerm.get(), uri, lastLogIndex, lastLogTerm, fromPreferredLeader);
        List<URI> destinations =  state.getConfigState().voters().stream()
                .filter(uri -> !uri.equals(this.uri)).collect(Collectors.toList());

        final AtomicBoolean isWinTheElection = new AtomicBoolean(false);
        final AtomicInteger votesGrantedInNewConfig = new AtomicInteger(0);
        final AtomicInteger votesGrantedInOldConfig = new AtomicInteger(0);

        updateVotes(isWinTheElection, votesGrantedInNewConfig, votesGrantedInOldConfig, this.uri);

        if (!isWinTheElection.get()) {
            final AtomicInteger pendingRequests = new AtomicInteger(destinations.size());
            for (URI destination : destinations) {
                getServerRpc(destination)
                        .thenComposeAsync(serverRpc -> {
                            if (null != serverRpc) {
                                logger.info("Request vote, dest uri: {}, {}...", serverRpc.serverUri(), voterInfo());
                                return serverRpc.requestVote(request)
                                        .thenApply(response -> {
                                            response.setUri(serverRpc.serverUri());
                                            return response;
                                        });
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }, asyncExecutor)
                        .thenAccept(response -> {
                            if (null != response) {
                                logger.info("Request vote result {}, dest uri: {}, {}...",
                                        response.isVoteGranted(),
                                        response.getUri(),
                                        voterInfo());
                                if(response.isVoteGranted()) {
                                    updateVotes(isWinTheElection, votesGrantedInNewConfig, votesGrantedInOldConfig, response.getUri());
                                }
                            }
                        }).exceptionally(e -> {
                            logger.warn("Request vote exception: {}!", e.getMessage());
                            return null;
                        }).thenRun(() -> {
                            if(pendingRequests.decrementAndGet() == 0 && !isWinTheElection.get()) {
                                electionTimeoutMs = config.getElectionTimeoutMs() + randomInterval(config.getElectionTimeoutMs());
                                nextElectionTime = System.currentTimeMillis() + electionTimeoutMs;
                            }
                });
            }
        }
    }

    private void updateVotes(AtomicBoolean isWinTheElection, AtomicInteger votesGrantedInNewConfig, AtomicInteger votesGrantedInOldConfig, URI destination) {
        ConfigState configState = state.getConfigState();
        if(configState.getConfigNew().contains(destination)) {
            votesGrantedInNewConfig.incrementAndGet();
        }
        if(configState.getConfigOld().contains(destination)) {
            votesGrantedInOldConfig.incrementAndGet();
        }

        boolean win;

        if (configState.isJointConsensus()) {
            win  = votesGrantedInNewConfig.get() >= configState.getConfigNew().size() / 2 + 1 &&
                    votesGrantedInOldConfig.get() >= configState.getConfigOld().size() / 2 + 1;
        } else {
            win = votesGrantedInNewConfig.get() >= configState.getConfigNew().size() / 2 + 1;
        }
        if(isWinTheElection.compareAndSet(false, win) && win) {
            convertToLeader();
        }
    }


    private void convertToCandidate() {
        synchronized (voterState) {
            VoterState oldState = voterState.getState();
            if(oldState == VoterState.FOLLOWER && null != follower) {
                follower.stop();
                follower = null;
            }
            voterState.convertToCandidate();
            logger.info("Convert voter state from {} to CANDIDATE, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
        }
    }


    /**
     * 将状态转换为Leader
     */
    private void convertToLeader() {
        synchronized (voterState) {
            VoterState oldState = voterState.getState();
            voterState.convertToLeader();

            this.leader = new Leader<>(journal, state, snapshots,currentTerm.get(),
                    uri, config.getCacheRequests(), config.getHeartbeatIntervalMs(), config.getRpcTimeoutMs(),
                    config.getReplicationParallelism(),config.getReplicationBatchSize(),
                    config.getSnapshotIntervalSec(), entryResultSerializer, threads,
                    this, this, asyncExecutor, scheduledExecutor, voterConfigManager, this, this,
                    this.journalEntryParser, config.getTransactionTimeoutMs(), snapshots);
            leader.start();
            this.leaderUri = this.uri;

            logger.info("Convert voter state from {} to LEADER, {}.", oldState, voterInfo());

        }

    }

    @Override
    protected void onJournalRecovered(Journal journal) {
        super.onJournalRecovered(journal);
        maybeUpdateTermOnRecovery(journal);
    }

    private void maybeUpdateTermOnRecovery(Journal journal) {
        if(journal.minIndex() < journal.maxIndex()) {
            JournalEntry lastEntry = journal.read(journal.maxIndex() - 1);
            if(lastEntry.getTerm() > currentTerm.get()) {
                currentTerm.set(lastEntry.getTerm());
                logger.info("Set current term to {}, this is the term of the last entry in the journal.",
                        currentTerm.get());
            }
        }
    }

    private void convertToFollower() {
        synchronized (voterState) {
            VoterState oldState = voterState.getState();
            if(oldState == VoterState.LEADER && null != leader) {
                leader.stop();
                leader = null;
            }
            voterState.convertToFollower();

            if(oldState == VoterState.FOLLOWER && null != follower) {
                follower.stop();
                follower = null;
            }

            follower = new Follower(journal, state, uri, currentTerm.get(),
                    voterConfigManager, threads,
                    snapshots, config.getCacheRequests());
            follower.start();

            this.electionTimeoutMs = config.getElectionTimeoutMs() + randomInterval(config.getElectionTimeoutMs());
            logger.info("Convert voter state from {} to FOLLOWER, electionTimeout: {}, {}.", oldState, electionTimeoutMs, voterInfo());
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
    @SuppressWarnings("unchecked")
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

        if (null != request.getLeader() && !request.getLeader().equals(leaderUri)) {
            leaderUri = request.getLeader();
        }

        return follower.addAppendEntriesRequest(request);
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
            synchronized (voteRequestMutex) {
                logger.info("RequestVoteRpc received: term: {}, candidate: {}, " +
                                "lastLogIndex: {}, lastLogTerm: {}, fromPreferredLeader: {}, {}.",
                        request.getTerm(), request.getCandidate(),
                        request.getLastLogIndex(), request.getLastLogTerm(), request.isFromPreferredLeader(), voterInfo());
                String rejectMsg = null;
                int currentTerm = this.currentTerm.get();
                // 来自推荐Leader的投票请求例外
                if (!request.isFromPreferredLeader()) {
                    // 如果当前是LEADER那直接拒绝投票

                    if (voterState() == VoterState.LEADER) {
                        rejectMsg = "I'm the leader";
                        return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
                    }
                    // 如何上次收到心跳的至今小于最小选举超时，拒绝投票
                    if (System.currentTimeMillis() - lastHeartbeat < config.getElectionTimeoutMs()) {
                        rejectMsg = "An election timeout not passed since last heartbeat received";
                        return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
                    }
                }
                if (request.getTerm() < currentTerm) {
                    rejectMsg = String.format("The candidate's term %d less than my term %d.",
                            request.getTerm(), currentTerm);
                    return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
                }

                checkTerm(request.getTerm());
                currentTerm = this.currentTerm.get();

                if (votedFor != null && !votedFor.equals(request.getCandidate())) {
                    rejectMsg = "Already vote to " + votedFor.toString();
                    return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
                }

                final long finalMaxJournalIndex = journal.maxIndex();
                final int lastLogTerm = journal.getTerm(finalMaxJournalIndex - 1);
                if ((request.getLastLogTerm() <= lastLogTerm
                        && (request.getLastLogTerm() != lastLogTerm
                        || request.getLastLogIndex() < finalMaxJournalIndex - 1))) {
                    rejectMsg = "Candidate’s log is at least as up-to-date as my log";
                    return rejectAndResponse(currentTerm, request.getCandidate(), rejectMsg);
                }


                logger.info("Grant vote to candidate {}, {}.", request.getCandidate(), voterInfo());
                this.votedFor = request.getCandidate();
                // 重置选举超时
//                lastHeartbeat = System.currentTimeMillis();
                return new RequestVoteResponse(currentTerm, true);
            }
        }, asyncExecutor);
    }

    private RequestVoteResponse rejectAndResponse(int term, URI candidate, String rejectMessage) {
        logger.info("Reject vote request from candidate {}, cause: [{}], {}.", candidate, rejectMessage, voterInfo());
        return new RequestVoteResponse(term, false);
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {

        return CompletableFuture.supplyAsync(() -> {
            if(voterState() == VoterState.LEADER && null != leader) {
                leader.disableWrite(request.getTimeoutMs(), request.getTerm());
            } else {
                throw new NotLeaderException(leaderUri);
            }
            return new DisableLeaderWriteResponse(currentTerm.get());
        }, asyncExecutor).exceptionally(DisableLeaderWriteResponse::new);
    }

    //Receiver implementation:
    //1. Reply immediately if term < currentTerm
    //2. Create new snapshot file if first chunk (offset is 0)
    //3. Write data into snapshot file at given offset
    //4. Reply and wait for more data chunks if done is false
    //5. Save snapshot file, discard any existing or partial snapshot
    //with a smaller index
    //6. If existing log entry has same index and term as snapshot’s
    //last included entry, retain log entries following it and reply
    //7. Discard the entire log
    //8. Reset state machine using snapshot contents (and load
    //snapshot’s cluster configuration)
    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        JournalKeeperState<E, ER, Q, QR> snapshot;
        if(checkTerm(request.getTerm())) {
            return CompletableFuture.completedFuture(new InstallSnapshotResponse(currentTerm.get()));
        }
        return installSnapshotAsync(request);
    }

    private CompletableFuture<InstallSnapshotResponse> installSnapshotAsync(InstallSnapshotRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            InstallSnapshotResponse response;

            try {
                installSnapshot(request.getOffset(), request.getLastIncludedIndex(),
                        request.getLastIncludedTerm(), request.getData(), request.isDone());
                response = new InstallSnapshotResponse(currentTerm.get());
            } catch (Throwable t) {
                logger.warn("Install snapshot exception!", t);
                response = new InstallSnapshotResponse(t);
            }
            return response;
        }, asyncExecutor);
    }


    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        Leader<E, ER, Q, QR> finalLeader = leader;
        try {
            ensureLeadership(finalLeader);
            return finalLeader.updateClusterState(request)
                    .exceptionally(UpdateClusterStateResponse::new);

        } catch (Throwable e) {
            return CompletableFuture.completedFuture(new UpdateClusterStateResponse(e));
        }
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return waitLeadership()
                .thenApply(aVoid -> state.query(querySerializer.parse(request.getQuery()), journal).getResult())
                .thenApply(resultSerializer::serialize)
                .thenApply(QueryStateResponse::new)
                .exceptionally(exception -> {
                    try {
                        throw exception;
                    } catch (NotLeaderException e) {
                        return new QueryStateResponse(new NotLeaderException(leaderUri));
                    } catch (Throwable t) {
                        return new QueryStateResponse(t);
                    }
                });
    }

    @Override
    public boolean checkTerm(int term) {
        boolean isTermChanged;
        synchronized (currentTerm) {
            if (term > currentTerm.get()) {
                logger.info("Set current term from {} to {}, {}.", currentTerm.get(), term, voterInfo());
                currentTerm.set(term);
                this.votedFor = null;

                isTermChanged = true;
            } else {
                isTermChanged = false;
            }

        }
        if(isTermChanged) {
            convertToFollower();
        }
        return isTermChanged;
    }

    // 改为同步方法，提升性能
    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {

        return waitLeadership()
                .thenCompose(aVoid -> CompletableFuture.supplyAsync(() -> new LastAppliedResponse(state.lastApplied())))
                .exceptionally(exception -> {
                    try {
                        throw exception;
                    } catch (NotLeaderException e) {
                        return new LastAppliedResponse(new NotLeaderException(leaderUri));
                    } catch (Throwable t) {
                        return new LastAppliedResponse(t);
                    }
                });
    }

    private CompletableFuture<Void> waitLeadership() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Leader<E, ER, Q, QR> finalLeader = leader;
        try {
            ensureLeadership(finalLeader);
        } catch (NotLeaderException nle) {
            future.completeExceptionally(nle);
            return future;
        }
        return finalLeader.waitLeadership();
    }
    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return CompletableFuture.supplyAsync(() -> new ServerStatus(
                Roll.VOTER,
                journal.minIndex(),
                journal.maxIndex(),
                journal.commitIndex(),
                state.lastApplied(),
                voterState()), asyncExecutor)
                .thenApply(GetServerStatusResponse::new);
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
     * * 当节点删除未提交的日志时，如果被删除的日志中包含配置变更，需要将当前节点的配置也一并回滚；¡
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
                .thenApply(InternalEntriesSerializeSupport::serialize)
                .thenApply(entry -> new UpdateClusterStateRequest(new SerializedUpdateRequest(entry, INTERNAL_PARTITION, 1)))
                .thenCompose(this::updateClusterState)
                .thenAccept(response -> {
                    if(!response.success()) {
                        throw new CompletionException(new UpdateConfigurationException("Failed to update voters configuration in step 1. " + response.errorString()));
                    }
                })
                .thenApply(aVoid -> new UpdateVotersResponse())
                .exceptionally(UpdateVotersResponse::new);
    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        if(voterState.getState() == VoterState.LEADER && leader != null) {
            return leader.createTransaction(request.getContext())
                    .thenApply(context -> new CreateTransactionResponse((UUIDTransactionId )context.transactionId(), context.timestamp()));
        } else {
            return CompletableFuture.completedFuture(new CreateTransactionResponse(new NotLeaderException(leaderUri)));
        }
    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        if(voterState.getState() == VoterState.LEADER && leader != null) {
            return leader.completeTransaction(request.getTransactionId(), request.isCommitOrAbort())
                    .thenApply(aVoid -> new CompleteTransactionResponse());
        } else {
            return CompletableFuture.completedFuture(new CompleteTransactionResponse(new NotLeaderException(leaderUri)));
        }
    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        if(voterState.getState() == VoterState.LEADER && leader != null) {
            return CompletableFuture.completedFuture(leader.getOpeningTransactions())
                    .thenApply(GetOpeningTransactionsResponse::new);
        } else {
            return CompletableFuture.completedFuture(new GetOpeningTransactionsResponse(new NotLeaderException(leaderUri)));
        }
    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        if (voterState.getState() == VoterState.LEADER && leader != null) {
            return CompletableFuture.completedFuture(
                    snapshots.values()
                            .stream()
                            .map((state) ->
                                    new SnapshotEntry(state.getPath().toString(), state.getLastIncludedIndex(),
                                            state.getLastIncludedTerm(), state.getMinOffset(), state.timestamp())).collect(Collectors.toList()))
                    .thenApply(SnapshotsEntry::new)
                    .thenApply(GetSnapshotsResponse::new);
        } else {
            return CompletableFuture.completedFuture(new GetSnapshotsResponse(new NotLeaderException(leaderUri)));
        }
    }

    private void ensureLeadership(Leader<E, ER, Q, QR> finalLeader) {
        if(voterState() != VoterState.LEADER || finalLeader == null) {
            throw new NotLeaderException(leaderUri);
        }
    }

    private VoterState voterState() {
        return voterState.getState();
    }

    @Override
    public void doStart() {
        convertToFollower();
        this.checkElectionTimeoutFuture = scheduledExecutor.scheduleAtFixedRate(this::checkElectionTimeout,
                ThreadLocalRandom.current().nextLong(500L, 1000L),
                config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);

        if(config.getPrintStateIntervalSec() > 0) {
            this.printStateFuture = scheduledExecutor.scheduleAtFixedRate(this::printState,
                    ThreadLocalRandom.current().nextLong(0, config.getPrintStateIntervalSec()),
                    config.getPrintStateIntervalSec(), TimeUnit.SECONDS);
        }

    }

    private void printState() {
        logger.info(voterInfo());
    }

    @Override
    public void doStop() {
        try {
            stopAndWaitScheduledFeature(checkElectionTimeoutFuture, 1000L);
            stopAndWaitScheduledFeature(printStateFuture, 1000L);
            if(null != leader) {
                leader.stop();
            }
            if(null != follower) {
                follower.stop();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            logger.warn("Exception, {}: ", voterInfo(), t);
        }

    }

    @Override
    protected void afterStateChanged(ER updateResult) {
        super.afterStateChanged(updateResult);
        if(null != leader) {
            try {
                leader.callback(state.lastApplied(), updateResult);
            } catch (Throwable e) {
                logger.warn("Callback exception! {}", voterInfo(), e);
            }
        }
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
        return String.format("VoterState: %s, currentTerm: %d, minIndex: %d, " +
                        "maxIndex: %d, commitIndex: %d, lastApplied: %d, uri: %s",
                voterState.getState(), currentTerm.get(), journal.minIndex(),
                journal.maxIndex(), journal.commitIndex(), state.lastApplied(), uri.toString());
    }

    private boolean checkPreferredLeader() {
        if (voterState().equals(VoterState.FOLLOWER) && serverUri().equals(state.getPreferredLeader()) && null != follower &&
                follower.getLeaderMaxIndex() - journal.maxIndex() < PREFERRED_LEADER_IN_SYNC_THRESHOLD && follower.getLeaderMaxIndex() > 0) {
            // 给当前LEADER发RPC，停服。
            logger.info("Send DisableLeaderWriteRequest to {}, {}", leaderUri, voterInfo());
            getServerRpc(leaderUri)
                    .thenComposeAsync(serverRpc -> serverRpc.disableLeaderWrite(new DisableLeaderWriteRequest(10 * config.getElectionTimeoutMs(), currentTerm.get())), asyncExecutor)
                    .thenAccept(response -> {
                        if (response.success() && response.getTerm() == currentTerm.get() &&
                                voterState() == VoterState.FOLLOWER && follower != null) {
                            logger.info("Received DisableLeaderWriteResponse code: SUCCESS, {}",
                                    voterInfo());
                            follower.setReadyForStartPreferredLeaderElection(true);

                        } else {
                            logger.info("Ignore DisableLeaderWriteResponse code: {}, term: {}, errString: {}, {}",
                                    response.getStatusCode(), response.getTerm(), response.errorString(), voterInfo());
                        }
                    });
        }

        // 等待数据完全同步
        // 发起选举，等待赢得足够的选票，成功新的LEADER

        return (voterState().equals(VoterState.FOLLOWER) && serverUri().equals(state.getPreferredLeader()) && null != follower &&
                follower.isReadyForStartPreferredLeaderElection() && follower.getLeaderMaxIndex() == journal.maxIndex());

    }

    // for monitor only
    VoterState getVoterState() {
        synchronized (voterState) {
            return voterState.getState();
        }
    }
    URI getLastVote() {
        return votedFor;
    }

    long getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    long getLastHeartbeat() {
        return lastHeartbeat;
    }

    URI getPreferredLeader() {
        return state.getPreferredLeader();
    }

    Leader getLeader() {
        return leader;
    }

    Follower getFollower() {
        return follower;
    }

    long getNextElectionTime() {
        return nextElectionTime;
    }

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

    public static class Config extends AbstractServer.Config {
        public final static long DEFAULT_HEARTBEAT_INTERVAL_MS = 100L;
        public final static long DEFAULT_ELECTION_TIMEOUT_MS = 300L;
        public final static int DEFAULT_REPLICATION_BATCH_SIZE = 128;
        public final static int DEFAULT_REPLICATION_PARALLELISM = 16;
        public final static int DEFAULT_CACHE_REQUESTS = 1024;
        public final static long DEFAULT_TRANSACTION_TIMEOUT_MS = 10L * 60 * 1000;
        public final static int DEFAULT_PRINT_STATE_INTERVAL_SEC = 0;

        public final static String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval_ms";
        public final static String ELECTION_TIMEOUT_KEY = "election_timeout_ms";
        public final static String REPLICATION_BATCH_SIZE_KEY = "replication_batch_size";
        public final static String REPLICATION_PARALLELISM_KEY = "replication_parallelism";
        public final static String CACHE_REQUESTS_KEY = "cache_requests";
        public final static String TRANSACTION_TIMEOUT_MS_KEY = "transaction_timeout_ms";
        public final static String PRINT_STATE_INTERVAL_SEC_KEY = "print_state_interval_sec";

        private long heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        private long electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;  // 最小选举超时
        private int replicationBatchSize = DEFAULT_REPLICATION_BATCH_SIZE;
        private int replicationParallelism = DEFAULT_REPLICATION_PARALLELISM;
        private int cacheRequests = DEFAULT_CACHE_REQUESTS;
        private long transactionTimeoutMs = DEFAULT_TRANSACTION_TIMEOUT_MS;
        private int printStateIntervalSec = DEFAULT_PRINT_STATE_INTERVAL_SEC;

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

        public long getTransactionTimeoutMs() {
            return transactionTimeoutMs;
        }

        public void setTransactionTimeoutMs(long transactionTimeoutMs) {
            this.transactionTimeoutMs = transactionTimeoutMs;
        }

        public int getPrintStateIntervalSec() {
            return printStateIntervalSec;
        }

        public void setPrintStateIntervalSec(int printStateIntervalSec) {
            this.printStateIntervalSec = printStateIntervalSec;
        }
    }


}
