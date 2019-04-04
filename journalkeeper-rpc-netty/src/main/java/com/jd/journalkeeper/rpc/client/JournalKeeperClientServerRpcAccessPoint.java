package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.RpcException;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.utils.UriUtils;

import java.net.URI;
import java.util.*;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ClientServerRpcStub> serverInstances = new HashMap<>();
    private URI currentServerUri = null;
    public JournalKeeperClientServerRpcAccessPoint(List<URI> servers, TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            this.transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        if(null != servers) {
            servers.forEach(server -> serverInstances.put(server, null));
        }
        this.properties = properties;
    }

    @Override
    public void updateServers(List<URI> uriList) {
        // 删除
        serverInstances.keySet().stream()
                .filter(uri -> !uriList.contains(uri))
                .map(serverInstances::remove)
                .forEach(this::disconnect);
        // 增加
        uriList.forEach(uri -> serverInstances.putIfAbsent(uri, null));


    }

    @Override
    public ClientServerRpc getClintServerRpc() {
        return getClintServerRpc(selectServer());
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if(null == uri ) return null;
        return serverInstances.computeIfAbsent(uri, this::connect);
    }

    @Override
    public void stop() {
        serverInstances.values()
                .forEach(this::disconnect);
        transportClient.stop();
    }

    private URI selectServer() {
        if(null == currentServerUri) {
            currentServerUri = serverInstances.entrySet().stream()
                    .filter(entry -> Objects.nonNull(entry.getValue()))
                    .filter(entry -> entry.getValue().isAlive())
                    .map(Map.Entry::getKey).findAny().
                            orElse(serverInstances.keySet().stream().findAny().orElse(null));
        }

        return currentServerUri;
    }

    private ClientServerRpcStub connect(URI server) {
        Transport transport = transportClient.createTransport(UriUtils.toSockAddress(server));
        return new ClientServerRpcStub(transport, server);
    }

    private void disconnect(ClientServerRpcStub clientServerRpc) {
        if(null != clientServerRpc) {
            clientServerRpc.stop();
        }
    }


}
