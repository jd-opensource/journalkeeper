package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.base.Queryable;
import com.jd.journalkeeper.base.Replicable;

import java.util.concurrent.CompletableFuture;

/**
 * Client调用Server的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpc<E,  S extends Replicable<S> & Queryable<Q, R>, Q, R> {

    CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateObserversRequest request);
    CompletableFuture<QueryStateResponse<R>> queryClusterState(QueryStateRequest<Q> request);
    CompletableFuture<QueryStateResponse<R>> queryServerState(QueryStateRequest<Q> request);
    CompletableFuture<LastAppliedResponse> lastApplied(LastAppliedRequest request);
    CompletableFuture<QueryStateResponse<R>> querySnapshot(QueryStateRequest<Q> request);
    CompletableFuture<GetServersResponse> getServer();
    CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request);
    CompletableFuture<UpdateObserversResponse> updateObservers(UpdateObserversRequest request);
}
