package com.jd.journalkeeper.core;

import com.jd.journalkeeper.core.server.Server;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;
import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;

import java.net.URI;

/**
 * 优先访问本地的ClientServerRpc接入点
 * @author liyue25
 * Date: 2019-03-25
 */
public class LocalDefaultRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Server server;
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    public LocalDefaultRpcAccessPoint(Server server, ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        this.server = server;
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
    }
    @Override
    public ClientServerRpc getClintServerRpc() {
        return server;
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        return clientServerRpcAccessPoint.getClintServerRpc(uri);
    }
}
