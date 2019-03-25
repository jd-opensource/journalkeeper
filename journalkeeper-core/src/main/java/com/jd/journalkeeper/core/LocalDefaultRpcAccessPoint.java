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
public class LocalDefaultRpcAccessPoint<E, Q, R> implements ClientServerRpcAccessPoint<E, Q, R> {
    private final Server<E, Q, R> server;

    public LocalDefaultRpcAccessPoint(Server<E, Q, R> server) {
        this.server = server;
    }

    @Override
    public ClientServerRpc<E, Q, R> getClintServerRpc() {
        return server;
    }

    @Override
    public ClientServerRpc<E, Q, R> getClintServerRpc(URI uri) {
        return null;
    }
}
