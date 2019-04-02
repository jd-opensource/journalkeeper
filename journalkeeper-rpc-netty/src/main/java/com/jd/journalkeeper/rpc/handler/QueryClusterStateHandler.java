package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.client.QueryStateResponse;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
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
public class QueryClusterStateHandler implements CommandHandler, Type {
    private final ServerRpc serverRpc;

    public QueryClusterStateHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        try {
            serverRpc.queryClusterState(GenericPayload.get(command.getPayload()))
                    .exceptionally(QueryStateResponse::new)
                    .thenAccept(response -> CommandSupport.sendResponse(response, QUERY_CLUSTER_STATE_RESPONSE, command, transport));
        } catch (Throwable throwable) {
            return CommandSupport.newResponseCommand(new QueryStateResponse(throwable), QUERY_CLUSTER_STATE_RESPONSE, command);
        }
        return null;
    }

    @Override
    public int type() {
        return QUERY_CLUSTER_STATE_REQUEST;
    }
}
