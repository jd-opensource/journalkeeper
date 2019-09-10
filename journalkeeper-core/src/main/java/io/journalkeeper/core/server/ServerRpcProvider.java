package io.journalkeeper.core.server;

import io.journalkeeper.rpc.server.ServerRpc;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
public interface ServerRpcProvider {
    CompletableFuture<ServerRpc> getServerRpc(URI uri);
}
