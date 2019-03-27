package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.core.api.StorageEntry;
import com.jd.journalkeeper.rpc.Detectable;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;

import java.util.concurrent.CompletableFuture;

/**
 * Server 各节点间的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ServerRpc extends ClientServerRpc, Detectable {
    CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request);
    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);
    CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request);
    CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request);
}
