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
package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.client.QueryStateResponse;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.*;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class QueryClusterStateHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public QueryClusterStateHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.queryClusterState(GenericPayload.get(command.getPayload()))
                    .exceptionally(QueryStateResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, QUERY_CLUSTER_STATE_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new QueryStateResponse(throwable), QUERY_CLUSTER_STATE_RESPONSE, command);
        }
        return null;
    }

    @Override
    public int type() {
        return QUERY_CLUSTER_STATE_REQUEST;
    }
}
