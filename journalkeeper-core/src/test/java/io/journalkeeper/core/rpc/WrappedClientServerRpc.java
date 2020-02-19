package io.journalkeeper.core.rpc;

import io.journalkeeper.rpc.client.AddPullWatchResponse;
import io.journalkeeper.rpc.client.CheckLeadershipResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
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
import io.journalkeeper.utils.event.EventWatcher;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class WrappedClientServerRpc implements ClientServerRpc {
    private final ClientServerRpc clientServerRpc;

    public WrappedClientServerRpc(ClientServerRpc clientServerRpc) {
        this.clientServerRpc = clientServerRpc;
    }

    @Override
    public URI serverUri() {
        return clientServerRpc.serverUri();
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return clientServerRpc.updateClusterState(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return clientServerRpc.queryClusterState(request);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return clientServerRpc.queryClusterState(request);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return clientServerRpc.lastApplied();
    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return clientServerRpc.queryClusterState(request);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return clientServerRpc.getServers();
    }

    @Override
    public CompletableFuture<GetServerStatusResponse> getServerStatus() {
        return clientServerRpc.getServerStatus();
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return clientServerRpc.addPullWatch();
    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return clientServerRpc.removePullWatch(request);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return clientServerRpc.updateVoters(request);
    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return clientServerRpc.pullEvents(request);
    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return clientServerRpc.convertRoll(request);
    }

    @Override
    public CompletableFuture<CreateTransactionResponse> createTransaction(CreateTransactionRequest request) {
        return clientServerRpc.createTransaction(request);
    }

    @Override
    public CompletableFuture<CompleteTransactionResponse> completeTransaction(CompleteTransactionRequest request) {
        return clientServerRpc.completeTransaction(request);
    }

    @Override
    public CompletableFuture<GetOpeningTransactionsResponse> getOpeningTransactions() {
        return clientServerRpc.getOpeningTransactions();
    }

    @Override
    public CompletableFuture<GetSnapshotsResponse> getSnapshots() {
        return clientServerRpc.getSnapshots();
    }

    @Override
    public CompletableFuture<CheckLeadershipResponse> checkLeadership() {
        return clientServerRpc.checkLeadership();
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        clientServerRpc.unWatch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        clientServerRpc.watch(eventWatcher);
    }

    @Override
    public void stop() {

    }
}
