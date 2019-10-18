package io.journalkeeper.core.client;

import io.journalkeeper.core.api.ServerConfigAware;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.utils.event.Watchable;
import io.journalkeeper.utils.retry.CompletableRetry;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019/10/16
 */
public interface ClientRpc extends ServerConfigAware, Watchable {
    <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke);
    <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(URI uri, CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke);

    <O extends BaseResponse> CompletableFuture<O> invokeClientLeaderRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke);

    URI getPreferredServer();

    void setPreferredServer(URI preferredServer);

    void stop();
}
