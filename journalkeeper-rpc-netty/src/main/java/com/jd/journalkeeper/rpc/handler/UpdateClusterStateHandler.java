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

import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
import static com.jd.journalkeeper.rpc.codec.RpcTypes.UPDATE_CLUSTER_STATE_RESPONSE;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class UpdateClusterStateHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public UpdateClusterStateHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.updateClusterState(GenericPayload.get(command.getPayload()))
                    .exceptionally(UpdateClusterStateResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, UPDATE_CLUSTER_STATE_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new UpdateClusterStateResponse(throwable), UPDATE_CLUSTER_STATE_RESPONSE, command);
        }
        return null;
    }

    @Override
    public int type() {
        return UPDATE_CLUSTER_STATE_REQUEST;
    }
}
