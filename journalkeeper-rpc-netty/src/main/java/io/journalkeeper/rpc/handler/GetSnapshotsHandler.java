package io.journalkeeper.rpc.handler;

import io.journalkeeper.rpc.client.GetSnapshotsResponse;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.utils.CommandSupport;

/**
 * GetSnapshotsHandler
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class GetSnapshotsHandler implements CommandHandler, Type {

    private final ServerRpc serverRpc;

    public GetSnapshotsHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int type() {
        return RpcTypes.GET_SNAPSHOTS_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.getSnapshots()
                    .exceptionally(GetSnapshotsResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.GET_SNAPSHOTS_RESPONSE, command, transport));
            return null;
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new GetSnapshotsResponse(throwable), RpcTypes.GET_SNAPSHOTS_RESPONSE, command);
        }
    }
}