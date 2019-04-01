package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.utils.CommandSupport;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.*;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class ServerRpcHandler implements CommandHandler, Types {
    private final ServerRpc serverRpc;

    public ServerRpcHandler(ServerRpc serverRpc) {
        this.serverRpc = serverRpc;
    }

    @Override
    public int[] types() {
        return new int[] {
            UPDATE_CLUSTER_STATE_REQUEST,
            QUERY_CLUSTER_STATE_REQUEST,
            QUERY_SERVER_STATE_REQUEST,
            LAST_APPLIED_REQUEST,
            QUERY_SNAPSHOT_REQUEST,
            GET_SERVERS_REQUEST
        };
    }

    @Override
    public Command handle(Transport transport, Command command) {
        JournalKeeperHeader header = (JournalKeeperHeader) command.getHeader();
        switch (header.getType()) {
            case UPDATE_CLUSTER_STATE_REQUEST:
                serverRpc.updateClusterState(GenericPayload.get(command.getPayload()))
                        .exceptionally(UpdateClusterStateResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, UPDATE_CLUSTER_STATE_RESPONSE, command, transport));
                break;
            case QUERY_CLUSTER_STATE_REQUEST:
                serverRpc.queryClusterState(GenericPayload.get(command.getPayload()))
                        .exceptionally(QueryStateResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, QUERY_CLUSTER_STATE_RESPONSE, command, transport));
                break;
            case QUERY_SERVER_STATE_REQUEST:
                serverRpc.queryClusterState(GenericPayload.get(command.getPayload()))
                        .exceptionally(QueryStateResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, QUERY_SERVER_STATE_RESPONSE, command, transport));
                break;
            case LAST_APPLIED_REQUEST:
                serverRpc.lastApplied()
                        .exceptionally(LastAppliedResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, LAST_APPLIED_RESPONSE, command, transport));
                break;
            case QUERY_SNAPSHOT_REQUEST:
                serverRpc.querySnapshot(GenericPayload.get(command.getPayload()))
                        .exceptionally(QueryStateResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, QUERY_SNAPSHOT_RESPONSE, command, transport));
                break;
            case GET_SERVERS_REQUEST:
                serverRpc.getServers()
                        .exceptionally(GetServersResponse::new)
                        .thenAccept(response -> CommandSupport.sendResponse(response, GET_SERVERS_RESPONSE, command, transport));
                break;
        }
        return null;
    }
}
