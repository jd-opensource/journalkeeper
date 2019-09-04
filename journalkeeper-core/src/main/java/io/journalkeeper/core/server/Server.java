package io.journalkeeper.core.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.rpc.client.AddPullWatchResponse;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.ConvertRollResponse;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.client.PullEventsRequest;
import io.journalkeeper.rpc.client.PullEventsResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.RemovePullWatchRequest;
import io.journalkeeper.rpc.client.RemovePullWatchResponse;
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
import io.journalkeeper.utils.event.EventWatcher;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019-09-03
 */
public class Server<E, ER, Q, QR>
        extends RaftServer<E, ER, Q, QR>
        implements ServerRpc {
    private AbstractServer<E, ER, Q, QR> server = null;

    private final Serializer<E> entrySerializer;
    private final Serializer<ER> entryResultSerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<QR> resultSerializer;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService asyncExecutor;
    private final Properties properties;
    public Server(Roll roll, StateFactory<E, ER, Q, QR> stateFactory, Serializer<E> entrySerializer, Serializer<ER> entryResultSerializer,
                  Serializer<Q> querySerializer, Serializer<QR> resultSerializer,
                  ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {
        super(stateFactory, properties);
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.properties = properties;
        this.server = createServer(roll);
    }

    private AbstractServer<E, ER, Q, QR> createServer(Roll roll) {
        switch (roll) {
            case VOTER:
                return new Voter<>(stateFactory, entrySerializer,entryResultSerializer, querySerializer, resultSerializer,scheduledExecutor, asyncExecutor, properties);
            default:
                return new Observer<>(stateFactory, entrySerializer,entryResultSerializer, querySerializer, resultSerializer,scheduledExecutor, asyncExecutor, properties);
        }
    }


    @Override
    public Roll roll() {
        return server.roll();
    }

    @Override
    public void init(URI uri, List<URI> voters) throws IOException {
        server.init(uri, voters);
    }

    @Override
    public void recover() throws IOException {
        server.recover();
    }

    @Override
    public URI serverUri() {
        return server.serverUri();
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return server.updateClusterState(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return server.queryClusterState(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return server.queryClusterState(request);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return server.lastApplied();
    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return server.querySnapshot(request);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return server.getServers();
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return server.getServerStatus();
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return server.addPullWatch();
    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return server.removePullWatch(request);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return server.updateVoters(request);
    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return server.pullEvents(request);
    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {

        return CompletableFuture.supplyAsync(()-> {
            if(request.getRoll() != null && request.getRoll() != server.roll()) {
                try {
                    if(server.serverState() != ServerState.RUNNING) {
                        throw  new IllegalStateException("Server is not running, current state: " + server.serverState() + "!");
                    }
                    server.stop();
                    server = createServer(request.getRoll());
                    server.recover();
                    server.start();
                } catch (Throwable t) {
                    return new ConvertRollResponse(t);
                }
            }
            return new ConvertRollResponse();
        }, asyncExecutor);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        server.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        server.unWatch(eventWatcher);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return server.asyncAppendEntries(request);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return server.requestVote(request);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return server.getServerEntries(request);
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return server.getServerState(request);
    }

    @Override
    public boolean isAlive() {
        return server.isAlive();
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop();
    }

    @Override
    public ServerState serverState() {
        return server.serverState();
    }
}
