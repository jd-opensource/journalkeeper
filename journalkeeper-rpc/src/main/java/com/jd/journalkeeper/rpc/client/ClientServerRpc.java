package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.Detectable;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * Client调用Server的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpc<E, Q, R> extends Detectable {

    URI serverUri();
    CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest<E> request);
    CompletableFuture<QueryStateResponse<R>> queryClusterState(QueryStateRequest<Q> request);
    CompletableFuture<QueryStateResponse<R>> queryServerState(QueryStateRequest<Q> request);
    CompletableFuture<LastAppliedResponse> lastApplied();
    CompletableFuture<QueryStateResponse<R>> querySnapshot(QueryStateRequest<Q> request);
    CompletableFuture<GetServersResponse> getServers();
}
