package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.Detectable;
import com.jd.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * Client调用Server的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpc extends Detectable {

    URI serverUri();
    CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request);
    CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request);
    CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request);
    CompletableFuture<LastAppliedResponse> lastApplied();
    CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request);
    CompletableFuture<GetServersResponse> getServers();

    void stop();
}
