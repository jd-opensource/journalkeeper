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
