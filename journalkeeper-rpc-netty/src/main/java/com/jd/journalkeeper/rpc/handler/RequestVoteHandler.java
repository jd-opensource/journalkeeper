package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import com.jd.journalkeeper.rpc.server.RequestVoteResponse;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;


/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RequestVoteHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public RequestVoteHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int type() {
        return RpcTypes.REQUEST_VOTE_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.requestVote(GenericPayload.get(command.getPayload()))
                    .exceptionally(RequestVoteResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, RpcTypes.REQUEST_VOTE_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new RequestVoteResponse(throwable), RpcTypes.REQUEST_VOTE_RESPONSE, command);
        }
        return null;
    }
}
