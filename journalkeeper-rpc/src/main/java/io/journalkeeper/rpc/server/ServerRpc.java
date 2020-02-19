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
package io.journalkeeper.rpc.server;

import io.journalkeeper.rpc.client.ClientServerRpc;

import java.util.concurrent.CompletableFuture;

/**
 * Server 各节点间的RPC
 * @author LiYue
 * Date: 2019-03-14
 */
public interface ServerRpc extends ClientServerRpc {
    CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request);

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request);

    CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request);

    CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request);
}
