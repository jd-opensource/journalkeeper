package io.journalkeeper.core.rpc;

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

import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class WrappedServerRpc extends WrappedClientServerRpc implements ServerRpc {
    private final ServerRpc serverRpc;

    public WrappedServerRpc(ServerRpc serverRpc) {
        super(serverRpc);
        this.serverRpc = serverRpc;
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return serverRpc.asyncAppendEntries(request);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return serverRpc.requestVote(request);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return serverRpc.getServerEntries(request);
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return serverRpc.getServerState(request);
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return serverRpc.disableLeaderWrite(request);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return serverRpc.installSnapshot(request);
    }
}
