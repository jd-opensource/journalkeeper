package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.api.StorageEntry;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.exceptions.NotVoterException;
import com.jd.journalkeeper.persistence.ServerMetadata;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.server.*;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Observer<E, Q, R> extends Server<E, Q, R> {
    private static final Logger logger = LoggerFactory.getLogger(Observer.class);
    /**
     * 父节点
     */
    private List<URI> parents;
    /**
     * 当前连接的父节点RPC代理
     */
    private ServerRpc<E, Q, R> currentServer = null;
    /**
     * 复制线程
     */
    private final LoopThread replicationThread;

    private final Config config;

    public Observer(StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {
        super(stateFactory, entrySerializer, scheduledExecutor, asyncExecutor, properties);
        this.config = toConfig(properties);
        replicationThread = buildReplicationThread();
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setPullBatchSize(Integer.parseInt(
                properties.getProperty(
                        Config.PULL_BATCH_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_PULL_BATCH_SIZE))));
        return config;
    }
    private LoopThread buildReplicationThread() {
        return LoopThread.builder()
                .name(String.format("ObserverReplicationThread-%s", uri.toString()))
                .doWork(this::pullEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("ObserverReplicationThread Exception: ", e))
                .build();
    }

    private void pullEntries() throws Throwable {
        if(null == currentServer) {
            currentServer = selectServer();
        }
        GetServerEntriesResponse<StorageEntry<E>> response =
                currentServer.getServerEntries(new GetServerEntriesRequest(commitIndex,config.getPullBatchSize())).get();

        try {
            if (null != response.getException()) {
                throw response.getException();
            }

            journal.append(response.getEntries());
            commitIndex += response.getEntries().size();
            // 唤醒状态机线程
            stateMachineThread.weakup();
        } catch (IndexUnderflowException e) {
            reset();
        } catch (IndexOverflowException ignored) {}


    }

    private void reset() throws InterruptedException, java.util.concurrent.ExecutionException, IOException {
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
            for(State<E, Q, R> snapshot: snapshots.values()) {
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

            // 复制远端服务器的最新装的到当前状态
            long lastIncludedIndex = -1;
            long offset = 0;
            boolean done;
            do {
                GetServerStateResponse r =
                        currentServer.getServerState(new GetServerStateRequest(lastIncludedIndex, offset)).get();
                state.install(r.getData(), r.getOffset());
                if(done = r.isDone()) {

                    state.installFinish(r.getLastIncludedIndex() + 1, r.getLastIncludedTerm());
                    journal.shrink(state.lastApplied());
                    commitIndex = state.lastApplied();

                    State<E, Q, R> snapshot = state.takeASnapshot(snapshotsPath().resolve(String.valueOf(state.lastApplied())));
                    snapshots.put(snapshot.lastApplied(), snapshot);
                }

            } while (!done);


        } finally {
            enable();
        }
    }

    private ServerRpc<E, Q, R> selectServer() {
        // TODO
        return null;
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
    public void start() {
        super.start();
        replicationThread.start();
    }

    @Override
    public void stop() {
        replicationThread.stop();
        super.stop();
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest<E> request) {
        return CompletableFuture.supplyAsync(() -> new UpdateClusterStateResponse(new NotLeaderException()), asyncExecutor);
    }

    @Override
    public CompletableFuture<QueryStateResponse<R>> queryClusterState(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> new QueryStateResponse<>(new NotLeaderException()), asyncExecutor);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CompletableFuture.supplyAsync(() -> new LastAppliedResponse(leader), asyncExecutor);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateVotersResponse(new NotLeaderException()), asyncExecutor);
    }

    @Override
    public CompletableFuture<UpdateObserversResponse> updateObservers(UpdateObserversRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateObserversResponse(new NotLeaderException()), asyncExecutor);
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

    public static class Config {
        public final static int DEFAULT_PULL_BATCH_SIZE = 4 * 1024 * 1024;
        public final static String PULL_BATCH_SIZE_KEY = "observer.pull_batch_size";

        private int pullBatchSize = DEFAULT_PULL_BATCH_SIZE;

        public int getPullBatchSize() {
            return pullBatchSize;
        }

        public void setPullBatchSize(int pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
        }
    }
}
