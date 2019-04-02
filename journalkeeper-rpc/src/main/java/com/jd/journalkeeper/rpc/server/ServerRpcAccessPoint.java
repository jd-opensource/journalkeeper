package com.jd.journalkeeper.rpc.server;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * 维护Server之间的RPC的接入点，维护rpc通道。
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ServerRpcAccessPoint {
    ServerRpc getServerRpcAgent(URI uri);
}
