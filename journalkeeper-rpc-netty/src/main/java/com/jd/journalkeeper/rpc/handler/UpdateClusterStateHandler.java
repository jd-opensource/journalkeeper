package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.StatusCode;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.command.Payload;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

import static com.jd.journalkeeper.rpc.client.ClientServerRpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
import static com.jd.journalkeeper.rpc.client.ClientServerRpcTypes.UPDATE_CLUSTER_STATE_RESPONSE;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class UpdateClusterStateHandler implements CommandHandler, Type {
    private final ClientServerRpc clientServerRpc;

    public UpdateClusterStateHandler(ClientServerRpc clientServerRpc) {
        this.clientServerRpc = clientServerRpc;
    }

    @Override
    public int type() {
        return UPDATE_CLUSTER_STATE_REQUEST;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        UpdateClusterStateRequest request = GenericPayload.get(command.getPayload());
        clientServerRpc.updateClusterState(request)
                .exceptionally(UpdateClusterStateResponse::new)
                .thenAccept(response -> {
                    JournalKeeperHeader header = new JournalKeeperHeader(Direction.RESPONSE, command.getHeader().getRequestId(), UPDATE_CLUSTER_STATE_RESPONSE);
                    header.setStatus(response.getStatusCode().getCode());
                    if(!response.success()) {
                        header.setError(response.getError());
                    }

                    Payload payload  = new GenericPayload<>(response);

                    Command responseCommand = new Command(header,payload);
                    transport.async(responseCommand);
                });
        return null;
    }
}
