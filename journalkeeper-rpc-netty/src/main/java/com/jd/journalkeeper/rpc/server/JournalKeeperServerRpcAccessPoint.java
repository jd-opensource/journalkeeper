package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.RpcException;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.utils.UriUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperServerRpcAccessPoint implements ServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ServerRpcStub> serverInstances = new HashMap<>();
    public JournalKeeperServerRpcAccessPoint(TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        this.properties = properties;
    }

    private ServerRpcStub connect(URI server) {
        Transport transport = transportClient.createTransport(UriUtils.toSockAddress(server));
        return new ServerRpcStub(transport, server);
    }

    @Override
    public ServerRpc getServerRpcAgent(URI uri) {
        if(null == uri ) return null;
        return serverInstances.computeIfAbsent(uri, this::connect);
    }

    @Override
    public void stop() {
        serverInstances.values().forEach(ServerRpcStub::stop);
        transportClient.stop();
    }
}
