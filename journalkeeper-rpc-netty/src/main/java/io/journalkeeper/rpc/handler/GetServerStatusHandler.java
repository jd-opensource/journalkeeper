package io.journalkeeper.rpc.handler;

import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.utils.CommandSupport;

/**
 * @author LiYue
 * Date: 2019-09-04
 */
public class GetServerStatusHandler implements CommandHandler, Type {
    private final ClientServerRpc clientServerRpc;

    public GetServerStatusHandler(ClientServerRpc clientServerRpc) {
        this.clientServerRpc = clientServerRpc;
    }


    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATUS_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            clientServerRpc.getServerStatus()
                    .exceptionally(GetServerStatusResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.GET_SERVER_STATUS_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new GetServerStatusResponse(throwable), RpcTypes.GET_SERVER_STATUS_RESPONSE, command);
        }
        return null;
    }
}
