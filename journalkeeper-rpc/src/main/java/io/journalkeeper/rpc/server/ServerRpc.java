package io.journalkeeper.rpc.server;

import java.util.concurrent.CompletableFuture;

/**
 * Server 各节点间的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ServerRpc {
    CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request);
    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);
    CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request);
}
