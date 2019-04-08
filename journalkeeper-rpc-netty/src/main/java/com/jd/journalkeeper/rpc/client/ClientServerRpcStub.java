package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.codec.RpcTypes;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.utils.CommandSupport;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static com.jd.journalkeeper.rpc.remoting.transport.TransportState.CONNECTED;

/**
 * 客户端桩
 * @author liyue25
 * Date: 2019-03-30
 */
public class ClientServerRpcStub implements ClientServerRpc {
    protected final Transport transport;
    protected final URI uri;
    public ClientServerRpcStub(Transport transport, URI uri) {
        this.transport = transport;
        this.uri = uri;
    }


    @Override
    public URI serverUri() {
        return uri;
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.UPDATE_CLUSTER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.QUERY_CLUSTER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.QUERY_SERVER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CommandSupport.sendRequest(null, RpcTypes.LAST_APPLIED_REQUEST, transport);

    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return CommandSupport
                .sendRequest(request, RpcTypes.QUERY_SNAPSHOT_REQUEST, transport);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return CommandSupport.sendRequest(null, RpcTypes.GET_SERVERS_REQUEST, transport);
    }

    @Override
    public boolean isAlive() {
        return null != transport  && transport.state() == CONNECTED;
    }

    @Override
    public void stop() {
        if(null != transport) {
            transport.stop();
        }
    }
}
