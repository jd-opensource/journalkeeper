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
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.NotVoterException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.persistence.ServerMetadata;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
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
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.IncreasingRetryPolicy;
import io.journalkeeper.utils.retry.RandomDestinationSelector;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
import java.util.stream.Collectors;

import static io.journalkeeper.core.server.MetricNames.METRIC_OBSERVER_REPLICATION;
import static io.journalkeeper.core.server.ThreadNames.OBSERVER_REPLICATION_THREAD;
import static io.journalkeeper.core.server.ThreadNames.STATE_MACHINE_THREAD;

/**
 * @author LiYue
 * Date: 2019-03-15
 */
class Observer<E, ER, Q, QR> extends AbstractServer<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(Observer.class);

    private final JMetric replicationMetric;

    private final CompletableRetry<URI> serverRpcRetry;
    private final Config config;

    Observer(StateFactory<E, ER, Q, QR> stateFactory,
                    Serializer<E> entrySerializer,
                    Serializer<ER> entryResultSerializer,
                    Serializer<Q> querySerializer,
                    Serializer<QR> queryResultSerializer,
                    JournalEntryParser journalEntryParser,
                    ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor,
                    ServerRpcAccessPoint serverRpcAccessPoint,
                    Properties properties) {
        super(stateFactory, entrySerializer, entryResultSerializer, querySerializer, queryResultSerializer,
                journalEntryParser, scheduledExecutor, asyncExecutor, serverRpcAccessPoint, properties);
        this.config = toConfig(properties);
        this.replicationMetric = getMetric(METRIC_OBSERVER_REPLICATION);
        threads.createThread(buildReplicationThread());
        serverRpcRetry = new CompletableRetry<>(new IncreasingRetryPolicy(new long [] {100, 500, 3000, 10000}, 50),
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
        if(null != parentsString) {
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
                .name(OBSERVER_REPLICATION_THREAD)
                .condition(() -> this.serverState() == ServerState.RUNNING)
                .doWork(this::pullEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("{} Exception: ", OBSERVER_REPLICATION_THREAD, e))
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
        }, asyncExecutor);
    }

    private void pullEntries() throws Throwable {

        replicationMetric.start();
        long traffic = 0L;
        GetServerEntriesResponse response =
                invokeParentsRpc(
                        rpc -> rpc.getServerEntries(new GetServerEntriesRequest(journal.commitIndex(),config.getPullBatchSize()))
                ).get();

        if(response.success()){

            journal.appendBatchRaw(response.getEntries());

            voterConfigManager.maybeUpdateNonLeaderConfig(response.getEntries(), votersConfigStateMachine);
//            commitIndex.addAndGet(response.getEntries().size());
            journal.commit(journal.maxIndex());
            // 唤醒状态机线程
            threads.wakeupThread(STATE_MACHINE_THREAD);
            traffic = response.getEntries().stream().mapToLong(bytes -> bytes.length).sum();


        } else if( response.getStatusCode() == StatusCode.INDEX_UNDERFLOW){
            reset();
        } else if( response.getStatusCode() != StatusCode.INDEX_OVERFLOW){
            logger.warn("Pull entry failed! {}", response.errorString());
        }
        replicationMetric.end(traffic);

    }

    private void reset() throws InterruptedException, ExecutionException, IOException {
//      INDEX_UNDERFLOW：Observer的提交位置已经落后目标节点太多，这时需要重置Observer，重置过程中不能提供读服务：
//        1. 删除log中所有日志和snapshots中的所有快照；
//        2. 将目标节点提交位置对应的状态复制到Observer上：parentServer.getServerState()，更新属性commitIndex和lastApplied值为返回值中的lastApplied。
        disable();
        try {
            // 删除状态
            if(state instanceof Closeable) {
                ((Closeable) state).close();
            }
            state.clear();

            // 删除所有快照
            for(State<E, ER, Q, QR> snapshot: snapshots.values()) {
                try {
                    if (snapshot instanceof Closeable) {
                        ((Closeable) snapshot).close();
                    }
                    snapshot.clear();
                } catch (Exception e) {
                    logger.warn("Clear snapshot at index: {} exception: ", snapshot.lastApplied(), e);
                }
            }
            snapshots.clear();

            // 复制远端服务器的最新状态到当前状态
            long lastIncludedIndex = -1;
            long offset = 0;
            boolean done;
            do {
                GetServerStateResponse r = invokeParentsRpc(
                        rpc -> rpc.getServerState(new GetServerStateRequest(lastIncludedIndex, offset))
                ).get();
                state.installSerializedData(r.getData(), r.getOffset());
                if(done = r.isDone()) {

                    state.installFinish(r.getLastIncludedIndex() + 1, r.getLastIncludedTerm());
                    journal.compact(state.lastApplied());

                    State<E, ER, Q, QR> snapshot = state.takeASnapshot(snapshotsPath().resolve(String.valueOf(state.lastApplied())), journal);
                    snapshots.put(snapshot.lastApplied(), snapshot);
                }

            } while (!done);


        } finally {
            enable();
        }
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
        threads.startThread(OBSERVER_REPLICATION_THREAD);
    }

    @Override
    public void doStop() {
        threads.stopThread(OBSERVER_REPLICATION_THREAD);

    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateClusterStateResponse(new NotLeaderException(leaderUri)), asyncExecutor);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return CompletableFuture.supplyAsync(() -> new QueryStateResponse(new NotLeaderException(leaderUri)), asyncExecutor);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CompletableFuture.supplyAsync(() -> new LastAppliedResponse(new NotLeaderException(leaderUri)), asyncExecutor);
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
        return CompletableFuture.supplyAsync(() -> new UpdateVotersResponse(new NotLeaderException(leaderUri)), asyncExecutor);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return CompletableFuture.supplyAsync(() -> new AsyncAppendEntriesResponse(new NotVoterException()), asyncExecutor);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CompletableFuture.supplyAsync(() -> new RequestVoteResponse(new NotVoterException()), asyncExecutor);
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return CompletableFuture.supplyAsync(() -> new DisableLeaderWriteResponse(new NotVoterException()), asyncExecutor);
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
