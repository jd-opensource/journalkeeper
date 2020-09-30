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

import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.exceptions.InstallSnapshotException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.NotVoterException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.CheckLeadershipResponse;
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
import io.journalkeeper.rpc.server.GetServerEntriesRequest;
import io.journalkeeper.rpc.server.GetServerEntriesResponse;
import io.journalkeeper.rpc.server.GetServerStateRequest;
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.retry.*;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.journalkeeper.core.server.MetricNames.METRIC_OBSERVER_REPLICATION;
import static io.journalkeeper.core.server.ThreadNames.OBSERVER_REPLICATION_THREAD;
import static io.journalkeeper.core.server.ThreadNames.STATE_MACHINE_THREAD;

/**
 * @author LiYue
 * Date: 2019-03-15
 */
class Observer extends AbstractServer {
    private static final Logger logger = LoggerFactory.getLogger(Observer.class);

    private final JMetric replicationMetric;

    private final CompletableRetry<URI> serverRpcRetry;
    private final Config config;

    Observer(StateFactory stateFactory,
             JournalEntryParser journalEntryParser,
             ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor,
             ServerRpcAccessPoint serverRpcAccessPoint,
             Properties properties) {
        super(stateFactory, journalEntryParser, scheduledExecutor, asyncExecutor, serverRpcAccessPoint, properties);
        this.config = toConfig(properties);
        this.replicationMetric = getMetric(METRIC_OBSERVER_REPLICATION);
        serverRpcRetry = new CompletableRetry<>(new ExponentialRetryPolicy(10L, 3000L, 10),
                new RandomDestinationSelector<>(config.getParents()));
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setPullBatchSize(Integer.parseInt(
                properties.getProperty(
                        Config.PULL_BATCH_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_PULL_BATCH_SIZE))));

        String parentsString = properties.getProperty(
                Config.PARENTS_KEY,
                null);
        if (null != parentsString) {
            config.setParents(Arrays.stream(parentsString.split(","))
                    .map(String::trim)
                    .map(URI::create).collect(Collectors.toList()));
        } else {
            logger.warn("Empty config {} in properties!", Config.PARENTS_KEY);
        }
        return config;
    }

    private AsyncLoopThread buildReplicationThread() {
        return ThreadBuilder.builder()
                .name(threadName(OBSERVER_REPLICATION_THREAD))
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::pullEntries)
                .sleepTime(50, 100)
                .onException(new DefaultExceptionListener(OBSERVER_REPLICATION_THREAD))
                .daemon(true)
                .build();
    }

    private <O extends BaseResponse> CompletableFuture<O> invokeParentsRpc(CompletableRetry.RpcInvoke<O, ServerRpc> invoke) {
        return serverRpcRetry.retry(uri -> invoke.invoke(serverRpcAccessPoint.getServerRpcAgent(uri)), new CheckRetry<O>() {
            @Override
            public boolean checkException(Throwable exception) {
                return true;
            }

            @Override
            public boolean checkResult(O result) {
                return !result.success();
            }
        }, asyncExecutor, scheduledExecutor);
    }

    private void pullEntries() throws Throwable {

        replicationMetric.start();
        long traffic = 0L;
        if (journal.commitIndex() == 0L) {
            installSnapshot(0L);
        }
        GetServerEntriesResponse response =
                invokeParentsRpc(
                        rpc -> rpc.getServerEntries(new GetServerEntriesRequest(journal.commitIndex(), config.getPullBatchSize()))
                ).get();

        if (response.success()) {

            journal.appendBatchRaw(response.getEntries());

            voterConfigManager.maybeUpdateNonLeaderConfig(response.getEntries(), state.getConfigState());
//            commitIndex.addAndGet(response.getEntries().size());
            journal.commit(journal.maxIndex());
            // 唤醒状态机线程
            threads.wakeupThread(threadName(STATE_MACHINE_THREAD));


        } else if (response.getStatusCode() == StatusCode.INDEX_UNDERFLOW) {
            installSnapshot(response.getMinIndex());
        } else if (response.getStatusCode() != StatusCode.INDEX_OVERFLOW) {
            logger.warn("Pull entry failed! {}", response.errorString());
        }
        replicationMetric.end(() -> response.getEntries().stream().mapToLong(bytes -> bytes.length).sum());

    }

    private void installSnapshot(long index) throws InterruptedException, ExecutionException, IOException, TimeoutException {
        // Observer的提交位置已经落后目标节点太多，这时需要安装快照：
        // 复制远端服务器的最新状态到当前状态
        long lastIncludedIndex = index - 1;
        int iteratorId = -1;
        boolean done;
        do {
            int finalIteratorId = iteratorId;
            GetServerStateResponse r = invokeParentsRpc(
                    rpc -> rpc.getServerState(new GetServerStateRequest(lastIncludedIndex, finalIteratorId))
            ).get();
            if (r.success()) {
                installSnapshot(r.getOffset(), r.getLastIncludedIndex(), r.getLastIncludedTerm(), r.getData(), r.isDone());
                iteratorId = r.getIteratorId();
                done = r.isDone();
            } else {
                throw new InstallSnapshotException(r.errorString());
            }
        } while (!done);
    }

    @Override
    public Roll roll() {
        return Roll.OBSERVER;
    }

    @Override
    protected void onMetadataRecovered(ServerMetadata metadata) {
        super.onMetadataRecovered(metadata);
        config.setParents(metadata.getParents());
    }

    @Override
    public void doStart() {
        threads.createThread(buildReplicationThread());
        threads.startThread(threadName(OBSERVER_REPLICATION_THREAD));
    }

    @Override
    public void doStop() {
        threads.stopThread(threadName(OBSERVER_REPLICATION_THREAD));

    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return CompletableFuture.completedFuture(new UpdateClusterStateResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return CompletableFuture.completedFuture(new QueryStateResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CompletableFuture.completedFuture(new LastAppliedResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return CompletableFuture.supplyAsync(() -> new ServerStatus(
                Roll.OBSERVER,
                journal.minIndex(),
                journal.maxIndex(),
                journal.commitIndex(),
                state.lastApplied(),
                null), asyncExecutor)
                .thenApply(GetServerStatusResponse::new);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return CompletableFuture.completedFuture(new UpdateVotersResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return CompletableFuture.completedFuture(new CreateTransactionResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return CompletableFuture.completedFuture(new CompleteTransactionResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return CompletableFuture.completedFuture(new GetOpeningTransactionsResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return CompletableFuture.completedFuture(new GetSnapshotsResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return CompletableFuture.completedFuture(new CheckLeadershipResponse(new NotLeaderException(leaderUri)));
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return CompletableFuture.completedFuture(new AsyncAppendEntriesResponse(new NotVoterException()));
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CompletableFuture.completedFuture(new RequestVoteResponse(new NotVoterException()));
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return CompletableFuture.completedFuture(new DisableLeaderWriteResponse(new NotVoterException()));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return CompletableFuture.completedFuture(new InstallSnapshotResponse(new NotVoterException()));
    }

    @Override
    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = super.createServerMetadata();
        serverMetadata.setParents(config.getParents());
        return serverMetadata;
    }

    private static class Config {
        private final static int DEFAULT_PULL_BATCH_SIZE = 4 * 1024 * 1024;
        private final static String PULL_BATCH_SIZE_KEY = "observer.pull_batch_size";

        private final static String PARENTS_KEY = "observer.parents";
        // TODO: 动态变更parents
        private List<URI> parents = Collections.emptyList();

        private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

        private int getPullBatchSize() {
            return pullBatchSize;
        }

        private void setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
        }

        public List<URI> getParents() {
            return parents;
        }

        public void setParents(List<URI> parents) {
            this.parents = parents;
        }
    }
}
