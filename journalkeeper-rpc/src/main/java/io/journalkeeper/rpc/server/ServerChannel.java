package io.journalkeeper.rpc.server;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * 维护Server之间的RPC通道
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ServerChannel {
    CompletableFuture<ServerRpc> getServerRpcAgent(URI uri);
}
