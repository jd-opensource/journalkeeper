/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.utils.CommandSupport;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static com.jd.journalkeeper.rpc.remoting.transport.TransportState.CONNECTED;

/**
 * 客户端桩
 * @author liyue25
 * Date: 2019-03-30
 */
public class ServerRpcStub  extends ClientServerRpcStub implements ServerRpc {
    public ServerRpcStub(Transport transport, URI uri) {
        super(transport, uri);
    }

    @Override
    public CompletableFuture<AsyncAppendEntriesResponse> asyncAppendEntries(AsyncAppendEntriesRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.ASYNC_APPEND_ENTRIES_REQUEST, transport);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.REQUEST_VOTE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<GetServerEntriesResponse> getServerEntries(GetServerEntriesRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.GET_SERVER_ENTRIES_REQUEST, transport);
    }

    @Override
    public CompletableFuture<GetServerStateResponse> getServerState(GetServerStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.GET_SERVER_STATE_REQUEST, transport);
    }
}
