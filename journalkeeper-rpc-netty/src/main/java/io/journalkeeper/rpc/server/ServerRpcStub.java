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

import io.journalkeeper.rpc.client.ClientServerRpcStub;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.TransportClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端桩
 * @author LiYue
 * Date: 2019-03-30
 */
public class ServerRpcStub extends ClientServerRpcStub implements ServerRpc {
    public ServerRpcStub(TransportClient transportClient, URI uri, InetSocketAddress inetSocketAddress, int version) {
        super(transportClient, uri, inetSocketAddress, version);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return sendRequest(request, RpcTypes.ASYNC_APPEND_ENTRIES_REQUEST);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return sendRequest(request, RpcTypes.REQUEST_VOTE_REQUEST);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return sendRequest(request, RpcTypes.GET_SERVER_ENTRIES_REQUEST);
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return sendRequest(request, RpcTypes.GET_SERVER_STATE_REQUEST);
    }

    @Override
    public CompletableFuture<DisableLeaderWriteResponse> disableLeaderWrite(DisableLeaderWriteRequest request) {
        return sendRequest(request, RpcTypes.DISABLE_LEADER_WRITE_REQUEST);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return sendRequest(request, RpcTypes.INSTALL_SNAPSHOT_REQUEST);
    }
}
