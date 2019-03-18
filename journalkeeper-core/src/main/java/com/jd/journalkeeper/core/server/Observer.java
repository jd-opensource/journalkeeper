package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Queryable;
import com.jd.journalkeeper.base.Replicable;
import com.jd.journalkeeper.core.api.StateMachine;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.exceptions.NotVoterException;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.server.*;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-03-15
 */
public class Observer<E,  S extends Replicable<S> & Queryable<Q, R>, Q, R> extends JournalKeeperServerAbstraction<E,S,Q,R> {
    private static final Logger logger = LoggerFactory.getLogger(Observer.class);
    private Set<URI> parents;
    private ServerRpc<E, S, Q, R> currentServer = null;
    private final LoopThread replicationThread;
    public Observer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine, Properties properties) {
        super(uri, voters, stateMachine, properties);
        replicationThread = buildReplicationThread();
    }

    public Observer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine) {
        super(uri, voters, stateMachine);
        replicationThread = buildReplicationThread();
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
        // TODO: 1024参数化
        GetServerEntriesResponse<E> response =
                currentServer.getServerEntries(new GetServerEntriesRequest(commitIndex,1024)).get();

        try {
            if (null != response.getException()) {
                throw response.getException();
            }

            journal.append(response.getEntries());
            commitIndex += response.getEntries().length;
        } catch (IndexUnderflowException e) {
//            INDEX_UNDERFLOW：Observer的提交位置已经落后目标节点太多，这时需要重置Observer，重置过程中不能提供读服务：
//            1. 删除log中所有日志和snapshots中的所有快照；
//            2. 将目标节点提交位置对应的状态复制到Observer上：parentServer.getServerState()，更新属性commitIndex和lastApplied值为返回值中的lastApplied。
            disable();
            try {
                GetStateResponse<S> stateResponse = currentServer.getServerState().get();
                this.state.set(stateResponse.getState(), stateResponse.getLastApplied());

                snapshots.clear();
                journal.shrink(response.getLastApplied());
                commitIndex = response.getLastApplied();

            } finally {
                enable();
            }
        } catch (IndexOverflowException ignored) {}


    }

    private ServerRpc<E, S, Q, R> selectServer() {
        // TODO
        return null;
    }

    @Override
    public Roll roll() {
        return Roll.OBSERVER;
    }

    @Override
    public void recover() {
        super.recover();
        // TODO
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
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateObserversRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateClusterStateResponse(new NotLeaderException()));
    }

    @Override
    public CompletableFuture<QueryStateResponse<R>> queryClusterState(QueryStateRequest<Q> request) {
        return CompletableFuture.supplyAsync(() -> new QueryStateResponse<>(new NotLeaderException()));
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied(LastAppliedRequest request) {
        return CompletableFuture.supplyAsync(() -> new LastAppliedResponse(new NotLeaderException()));
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateVotersResponse(new NotLeaderException()));
    }

    @Override
    public CompletableFuture<UpdateObserversResponse> updateObservers(UpdateObserversRequest request) {
        return CompletableFuture.supplyAsync(() -> new UpdateObserversResponse(new NotLeaderException()));
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return CompletableFuture.supplyAsync(() -> new AsyncAppendEntriesResponse(new NotVoterException()));
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CompletableFuture.supplyAsync(() -> new RequestVoteResponse(new NotVoterException()));
    }



}
