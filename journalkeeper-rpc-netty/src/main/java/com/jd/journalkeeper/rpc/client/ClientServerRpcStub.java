package com.jd.journalkeeper.rpc.client;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端桩
 * @author liyue25
 * Date: 2019-03-30
 */
public class ClientServerRpcStub implements ClientServerRpc {
    @Override
    public URI serverUri() {
        return null;
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return null;
    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return null;
    }

    @Override
    public boolean isAlive() {
        return false;
    }
}
