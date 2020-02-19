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
package io.journalkeeper.rpc.handler;

import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.payload.GenericPayload;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.server.DisableLeaderWriteResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.utils.CommandSupport;

/**
 * @author LiYue
 * Date: 2019-04-01
 */
public class DisableLeaderWriteRequestHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public DisableLeaderWriteRequestHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.disableLeaderWrite(GenericPayload.get(command.getPayload()))
                    .exceptionally(DisableLeaderWriteResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.DISABLE_LEADER_WRITE_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new DisableLeaderWriteResponse(throwable), RpcTypes.DISABLE_LEADER_WRITE_RESPONSE, command);
        }
        return null;
    }

    @Override
    public int type() {
        return RpcTypes.DISABLE_LEADER_WRITE_REQUEST;
    }
}
