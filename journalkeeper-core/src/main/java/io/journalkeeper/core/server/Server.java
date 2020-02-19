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
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.monitor.MonitorCollector;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.AddPullWatchResponse;
import io.journalkeeper.rpc.client.CheckLeadershipResponse;
import io.journalkeeper.rpc.client.CompleteTransactionRequest;
import io.journalkeeper.rpc.client.CompleteTransactionResponse;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.ConvertRollResponse;
import io.journalkeeper.rpc.client.CreateTransactionRequest;
import io.journalkeeper.rpc.client.CreateTransactionResponse;
import io.journalkeeper.rpc.client.GetOpeningTransactionsResponse;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.GetSnapshotsResponse;
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
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019-09-03
 */
public class Server
        implements ServerRpc, RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private final RpcAccessPointFactory rpcAccessPointFactory;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService asyncExecutor;
    private final Properties properties;
    private final StateFactory stateFactory;
    private final JournalEntryParser journalEntryParser;
    private final ServerMonitorInfoProvider serverMonitorInfoProvider;
    private final Collection<MonitorCollector> monitorCollectors;
    private AbstractServer server;
    private StateServer rpcServer = null;
    private ServerState serverState = ServerState.CREATED;
    private ServerRpcAccessPoint serverRpcAccessPoint;


    public Server(Roll roll, StateFactory stateFactory, JournalEntryParser journalEntryParser,
                  ScheduledExecutorService scheduledExecutor, ExecutorService asyncExecutor, Properties properties) {

        rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        this.scheduledExecutor = scheduledExecutor;
        this.asyncExecutor = asyncExecutor;
        this.stateFactory = stateFactory;
        this.properties = properties;
        this.serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(properties);
        this.journalEntryParser = journalEntryParser;
        this.server = createServer(roll);
        this.serverMonitorInfoProvider = new ServerMonitorInfoProvider(this);
        this.monitorCollectors = ServiceSupport.loadAll(MonitorCollector.class);
    }

    private AbstractServer createServer(Roll roll) {
        if (roll == Roll.VOTER) {
            return new Voter(stateFactory, journalEntryParser, scheduledExecutor, asyncExecutor, serverRpcAccessPoint, properties);
        }
        return new Observer(stateFactory, journalEntryParser, scheduledExecutor, asyncExecutor, serverRpcAccessPoint, properties);
    }

    @Override
    public Roll roll() {
        return server.roll();
    }

    @Override
    public void init(URI uri, List<URI> voters, Set<Integer> partitions, URI preferredLeader) throws IOException {
        server.init(uri, voters, partitions, preferredLeader);
    }

    @Override
    public boolean isInitialized() {
        return server.isInitialized();
    }

    @Override
    public void recover() throws IOException {
        server.recover();
    }

    @Override
    public URI serverUri() {
        return null == server ? null : server.serverUri();
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

        return CompletableFuture.supplyAsync(() -> {
            if (request.getRoll() != null && request.getRoll() != server.roll()) {
                try {
                    if (server.serverState() != ServerState.RUNNING) {
                        throw new IllegalStateException("Server is not running, current state: " + server.serverState() + "!");
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
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return server.createTransaction(request);
    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return server.completeTransaction(request);
    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return server.getOpeningTransactions();
    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return server.getSnapshots();
    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return server.checkLeadership();
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
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return server.disableLeaderWrite(request);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return server.installSnapshot(request);
    }

    private void addMonitorProviderToCollectors() {
        if (null != monitorCollectors) {
            for (MonitorCollector monitorCollector : monitorCollectors) {
                monitorCollector.addServer(serverMonitorInfoProvider);
            }
        }
    }

    private void removeMonitorProviderToCollectors() {
        if (null != monitorCollectors) {
            for (MonitorCollector monitorCollector : monitorCollectors) {
                monitorCollector.removeServer(serverMonitorInfoProvider);
            }
        }
    }

    @Override
    public void start() {
        addMonitorProviderToCollectors();
        if (this.serverState != ServerState.CREATED) {
            throw new IllegalStateException("Server can only start once!");
        }
        this.serverState = ServerState.STARTING;
        server.start();
        rpcServer = rpcAccessPointFactory.bindServerService(this);
        rpcServer.start();
        this.serverState = ServerState.RUNNING;

    }

    @Override
    public void stop() {
        if (this.serverState == ServerState.RUNNING) {

            this.serverState = ServerState.STOPPING;
            if (rpcServer != null) {
                rpcServer.stop();
            }
            server.stop();
            serverRpcAccessPoint.stop();
            this.serverState = ServerState.STOPPED;
            removeMonitorProviderToCollectors();
            logger.info("Server {} stopped.", serverUri());
        }
    }

    @Override
    public ServerState serverState() {
        return server.serverState();
    }

    AbstractServer getServer() {
        return server;
    }
}
