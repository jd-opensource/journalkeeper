package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Queryable;
import com.jd.journalkeeper.base.Replicable;
import com.jd.journalkeeper.base.event.EventWatcher;
import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.core.api.JournalKeeperClient;
import com.jd.journalkeeper.core.api.JournalKeeperServer;
import com.jd.journalkeeper.core.api.StateMachine;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.server.GetServerEntriesRequest;
import com.jd.journalkeeper.rpc.server.GetServerEntriesResponse;
import com.jd.journalkeeper.rpc.server.GetStateResponse;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Server就是集群中的节点，它包含了存储在Server上日志（journal），一组快照（snapshots[]）和一个状态机（stateMachine）实例。
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class JournalKeeperServerAbstraction<E,  S extends Replicable<S> & Queryable<Q, R>, Q, R>
        extends JournalKeeperServer<E, S, Q, R>
        implements ServerRpc<E, S, Q, R>, ClientServerRpc<E, S, Q, R> {
    private static final Logger logger = LoggerFactory.getLogger(JournalKeeperServerAbstraction.class);
    /**
     * 存放日志
     */
    protected Journal<E> journal;
    /**
     * 存放节点上所有状态快照的稀疏数组，数组的索引（key）就是快照对应的日志位置的索引
     */
    protected final NavigableMap<Long, S> snapshots = new ConcurrentSkipListMap<>();
    /**
     * 节点上的最新状态 和 被状态机执行的最大日志条目的索引值（从 0 开始递增）
     */
    protected StateWithIndex<E, S> state;

    /**
     * 已知的被提交的最大日志条目的索引值（从 0 开始递增）
     */
    protected long commitIndex;

    /**
     * 当前LEADER节点地址
     */
    protected URI leader;

    /**
     * 观察者节点
     */
    protected Set<URI> observers;

    /**
     * 每个Server模块中需要运行一个用于执行日志更新状态，保存Snapshot的状态机线程，
     */
    protected final LoopThread stateMachineThread;

    /**
     * 可用状态
     */
    private boolean available = false;

    protected void enable(){
        this.available = true;
    }

    protected void disable() {
        this.available = false;
    }

    protected boolean isAvailable() {
        return available;
    }

    private Config config;

    public JournalKeeperServerAbstraction(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine, Properties properties){
        super(uri, voters,stateMachine, properties);
        this.config = toConfig(properties);
        this.stateMachineThread = buildStateMachineThread();
    }
    public JournalKeeperServerAbstraction(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine){
        super(uri, voters,stateMachine);
        this.config = new Config();
        this.stateMachineThread = buildStateMachineThread();
    }

    private LoopThread buildStateMachineThread() {
        return LoopThread.builder()
                .name(String.format("StateMachineThread-%s", uri.toString()))
                .doWork(this::applyEntries)
                .sleepTime(50,100)
                .onException(e -> logger.warn("StateMachineThread Exception: ", e))
                .build();
    }


    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setSnapshotStep(Integer.parseInt(
                properties.getProperty(
                        Config.SNAPSHOT_STEP_KEY,
                        String.valueOf(Config.DEFAULT_SNAPSHOT_STEP))));
        return config;
    }

    /**
     * 监听属性commitIndex的变化，
     * 当commitIndex变更时如果commitIndex > lastApplied，
     * 反复执行applyEntries直到lastApplied == commitIndex：
     *
     * 1. 如果需要，复制当前状态为新的快照保存到属性snapshots, 索引值为lastApplied。
     * 2. lastApplied自增，将log[lastApplied]应用到状态机，更新当前状态state；
     *
     */
    private void applyEntries() {
        while ( state.getIndex() < commitIndex) {
            takeASnapShotIfNeed();
            E entry = journal.read(state.getIndex());
            state.execute(entry, stateMachine);
        }
    }

    /**
     * 如果需要，保存一次快照
     */
    private void takeASnapShotIfNeed() {
        StateWithIndex<E,S> snapshot = state.takeSnapshot();
        if(snapshots.isEmpty() || snapshot.getIndex() - snapshots.lastKey() > config.getSnapshotStep()) {
            snapshots.put(snapshot.getIndex(), snapshot.getState());
        }
    }

    @Override
    public CompletableFuture<QueryStateResponse<R>> queryServerState(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StateWithIndex<E, S> snapshot = state.takeSnapshot();
                return new QueryStateResponse<>(snapshot.getState().query(request.getQuery()).get(), snapshot.getIndex());
            } catch (Throwable throwable) {
                return new QueryStateResponse<>(throwable);
            }
        });
    }

    /**
     * 如果请求位置存在对应的快照，直接从快照中读取状态返回；如果请求位置不存在对应的快照，那么需要找到最近快照日志，以这个最近快照日志对应的快照为输入，从最近快照日志开始（不含）直到请求位置（含）依次在状态机中执行这些日志，执行完毕后得到的快照就是请求位置的对应快照，读取这个快照的状态返回给客户端即可。
     * 实现流程：
     *
     * 对比logIndex与在属性snapshots数组的上下界，检查请求位置是否越界，如果越界返回INDEX_OVERFLOW/INDEX_UNDERFLOW错误。
     * 查询snapshots[logIndex]是否存在，如果存在快照中读取状态返回，否则下一步；
     * 找到snapshots中距离logIndex最近且小于logIndex的快照位置和快照，记为nearestLogIndex和nearestSnapshot；
     * 从log中的索引位置nearestLogIndex + 1开始，读取N条日志，N = logIndex - nearestLogIndex获取待执行的日志数组execLogs[]；
     * 调用以nearestSnapshot为输入，依次在状态机stateMachine中执行execLogs，得到logIndex位置对应的快照，从快照中读取状态返回。
     */
    @Override
    public CompletableFuture<QueryStateResponse<R>> querySnapshot(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StateWithIndex<E, S> snapshot = state.takeSnapshot();
                if (request.getIndex() > snapshot.getIndex()) {
                    throw new IndexOverflowException();
                }

                if (request.getIndex() == snapshot.getIndex()) {
                    try {
                        return new QueryStateResponse<>(snapshot.getState().query(request.getQuery()).get(), snapshot.getIndex());
                    } catch (Throwable throwable) {
                        return new QueryStateResponse<>(throwable);
                    }
                }

                S requestState = snapshots.get(request.getIndex());
                if (null == requestState) {
                    Map.Entry<Long, S> nearestSnapshot = snapshots.floorEntry(request.getIndex());
                    if (null == nearestSnapshot) {
                        throw new IndexUnderflowException();
                    }

                    E[] toBeExecutedEntries = journal.read(nearestSnapshot.getKey(), (int) (request.getIndex() - nearestSnapshot.getKey()));
                    requestState = stateMachine.execute(nearestSnapshot.getValue(), toBeExecutedEntries);
                    snapshots.putIfAbsent(request.getIndex(), requestState);
                }
                return new QueryStateResponse<>(requestState.query(request.getQuery()).get());
            } catch (Throwable throwable) {
                return new QueryStateResponse<>(throwable);
            }
        });
    }

    @Override
    public CompletableFuture<GetServersResponse> getServer() {
        return CompletableFuture.supplyAsync(() -> new GetServersResponse(new ClusterConfiguration(leader, voters, observers)));
    }

    @Override
    public CompletableFuture<GetStateResponse<S>> getServerState() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StateWithIndex<E, S> snapshot = state.takeSnapshot();
                return new GetStateResponse<>(snapshot.getState(), snapshot.getIndex());
            } catch (Throwable throwable) {
                return new GetStateResponse<>(throwable);
            }
        });
    }

    @Override
    public void start() {
        stateMachineThread.start();
    }

    @Override
    public void stop() {
        stateMachineThread.stop();
    }

    @Override
    public CompletableFuture<JournalKeeperClient<Q, R, E>> connect(Set<URI> servers, Properties properties) {
        // TODO
        return null;
    }


    @Override
    public void watch(EventWatcher eventWatcher) {
        // TODO
    }

    @Override
    public void unwatch(EventWatcher eventWatcher) {
        // TODO
    }


    @Override
    public void recover() {
        // TODO
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse<E>> getServerEntries(GetServerEntriesRequest request) {
        return CompletableFuture.supplyAsync(() ->
                new GetServerEntriesResponse<>(
                        journal.read(request.getIndex(), request.getMaxSize()),
                        journal.minIndex(), state.getIndex()));
    }

    @Override
    public ServerState serverState() {
        return stateMachineThread.serverState();
    }

    public static class Config {
        public final static int DEFAULT_SNAPSHOT_STEP = 128;
        public final static String SNAPSHOT_STEP_KEY = "snapshot-step";

        private int snapshotStep = DEFAULT_SNAPSHOT_STEP;

        public int getSnapshotStep() {
            return snapshotStep;
        }

        public void setSnapshotStep(int snapshotStep) {
            this.snapshotStep = snapshotStep;
        }
    }
}
