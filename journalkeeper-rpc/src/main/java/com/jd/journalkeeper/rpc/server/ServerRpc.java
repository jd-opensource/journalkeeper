package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.core.api.StorageEntry;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;

import java.util.concurrent.CompletableFuture;

/**
 * Server 各节点间的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ServerRpc<E, Q, R> extends ClientServerRpc<E, Q, R> {
    CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest<StorageEntry<E>> request);
    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);
    CompletableFuture<GetServerEntriesResponse<StorageEntry<E>>> getServerEntries(GetServerEntriesRequest request);
    CompletableFuture<GetStateResponse> getServerState();
}
