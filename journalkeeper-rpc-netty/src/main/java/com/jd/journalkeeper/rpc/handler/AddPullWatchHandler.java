package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.client.AddPullWatchResponse;
import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;


/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class AddPullWatchHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public AddPullWatchHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int type() {
        return RpcTypes.ADD_PULL_WATCH_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.addPullWatch()
                    .exceptionally(AddPullWatchResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.ADD_PULL_WATCH_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new AddPullWatchResponse(throwable), RpcTypes.ADD_PULL_WATCH_RESPONSE, command);
        }
        return null;
    }
}
