package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.GetServerEntriesResponse;
import com.jd.journalkeeper.rpc.server.RequestVoteResponse;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;


/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class GetServerEntriesHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public GetServerEntriesHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_ENTRIES_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.getServerEntries(GenericPayload.get(command.getPayload()))
                    .exceptionally(GetServerEntriesResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.GET_SERVER_ENTRIES_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new GetServerEntriesResponse(throwable), RpcTypes.GET_SERVER_ENTRIES_RESPONSE, command);
        }
        return null;
    }
}
