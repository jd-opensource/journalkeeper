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
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.NotVoterException;
import io.journalkeeper.metric.JMetric;
import io.journalkeeper.persistence.ServerMetadata;
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
import io.journalkeeper.rpc.server.GetServerEntriesRequest;
import io.journalkeeper.rpc.server.GetServerEntriesResponse;
import io.journalkeeper.rpc.server.GetServerStateRequest;
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author LiYue
 * Date: 2019-03-15
 */
class Observer<E, ER, Q, QR> extends AbstractServer<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(Observer.class);
    private static final String OBSERVER_REPLICATION_THREAD = "ObserverReplicationThread";

    private static final String METRIC_OBSERVER_REPLICATION = "OBSERVER_REPLICATION";
    private final JMetric replicationMetric;
    /**
     * 父节点
     */
    private List<URI> parents;

    private ServerRpc currentServer = null;

    private final Config config;

    public Observer(StateFactory<E, ER, Q, QR> stateFactory,
                    Serializer<E> entrySerializer,
                    Serializer<ER> entryResultSerializer,
                    Serializer<Q> querySerializer,
                    Serializer<QR> queryResultSerializer,
                    ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor,
                    Properties properties) {
        super(stateFactory, entrySerializer, entryResultSerializer, querySerializer, queryResultSerializer, scheduledExecutor, asyncExecutor, properties);
        this.config = toConfig(properties);
        this.replicationMetric = getMetric(METRIC_OBSERVER_REPLICATION);
        threads.createThread(buildReplicationThread());
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setPullBatchSize(Integer.parseInt(
                properties.getProperty(
                        Config.PULL_BATCH_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_PULL_BATCH_SIZE))));
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

    @Override
    public synchronized void init(URI uri, List<URI> voters) throws IOException {
        parents = new ArrayList<>(voters);
        super.init(uri, voters);
    }

    private void pullEntries() throws Throwable {

        replicationMetric.start();
        long traffic = 0L;
        GetServerEntriesResponse response =
                selectServer().getServerEntries(new GetServerEntriesRequest(journal.commitIndex(),config.getPullBatchSize())).get();

        if(response.success()){

            journal.appendBatchRaw(response.getEntries());

            maybeUpdateConfigOnReplication(response.getEntries());
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
                GetServerStateResponse r =
                        selectServer().getServerState(new GetServerStateRequest(lastIncludedIndex, offset)).get();
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

    private ServerRpc selectServer() {
        long sleep = 0L;
        while (null == currentServer || !currentServer.isAlive()) {
            if(sleep > 0) {
                logger.info("Waiting {} ms to reconnect...", sleep);
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {}
            }
            if(null != currentServer) {
                currentServer.stop();
            }
            logger.info("Connecting server {}", uri);
            URI uri = parents.get(ThreadLocalRandom.current().nextInt(parents.size()));
            currentServer = serverRpcAccessPoint.getServerRpcAgent(uri);
            sleep += sleep + 100L;
        }

        return currentServer;
    }

    @Override
    public Roll roll() {
        return Roll.OBSERVER;
    }

    @Override
    protected void onMetadataRecovered(ServerMetadata metadata) {
        super.onMetadataRecovered(metadata);
        this.parents = metadata.getParents();
    }

    @Override
    public void doStart() {
    }

    @Override
    public void doStop() {
        if(null != currentServer) {
            currentServer.stop();
        }
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateClusterStateResponse(new NotLeaderException(leader)), asyncExecutor);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return CompletableFuture.supplyAsync(() -> new QueryStateResponse(new NotLeaderException(leader)), asyncExecutor);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CompletableFuture.supplyAsync(() -> new LastAppliedResponse(new NotLeaderException(leader)), asyncExecutor);
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
        return CompletableFuture.supplyAsync(() -> new UpdateVotersResponse(new NotLeaderException(leader)), asyncExecutor);
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
    protected ServerMetadata createServerMetadata() {
        ServerMetadata serverMetadata = super.createServerMetadata();
        serverMetadata.setParents(parents);
        return serverMetadata;
    }

    private static class Config {
        private final static int DEFAULT_PULL_BATCH_SIZE = 4 * 1024 * 1024;
        private final static String PULL_BATCH_SIZE_KEY = "observer.pull_batch_size";

        private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

        private int getPullBatchSize() {
            return pullBatchSize;
        }

        private void setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
        }
    }
}
