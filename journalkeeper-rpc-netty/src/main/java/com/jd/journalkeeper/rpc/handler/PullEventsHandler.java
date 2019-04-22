package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.client.PullEventsResponse;
import com.jd.journalkeeper.rpc.client.RemovePullWatchResponse;
import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;


/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class PullEventsHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public PullEventsHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int type() {
        return RpcTypes.PULL_EVENTS_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.pullEvents(GenericPayload.get(command.getPayload()))
                    .exceptionally(PullEventsResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.PULL_EVENTS_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new PullEventsResponse(throwable), RpcTypes.PULL_EVENTS_RESPONSE, command);
        }
        return null;
    }
}
