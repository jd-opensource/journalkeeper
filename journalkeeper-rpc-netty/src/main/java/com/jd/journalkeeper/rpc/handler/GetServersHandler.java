package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.client.GetServersResponse;
import com.jd.journalkeeper.rpc.client.LastAppliedResponse;
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
public class GetServersHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public GetServersHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.getServers()
                    .exceptionally(GetServersResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, GET_SERVERS_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new GetServersResponse(throwable), GET_SERVERS_RESPONSE, command);
        }
        return null;
    }

    @Override
    public int type() {
        return GET_SERVERS_REQUEST;
    }
}
