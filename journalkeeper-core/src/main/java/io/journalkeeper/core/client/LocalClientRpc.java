package io.journalkeeper.core.client;

import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.DestinationSelector;
import io.journalkeeper.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * 绑定到本地Server的Client，只访问本地Server
 * @author LiYue
 * Date: 2019/10/17
 */
public class LocalClientRpc implements ClientRpc {
    private static final Logger logger = LoggerFactory.getLogger(LocalClientRpc.class);
    private final ClientServerRpc localServer;
    private final CompletableRetry<URI> completableRetry;
    private final CheckRetry<BaseResponse> checkRetry = new LocalClientCheckRetry();
    private final Executor executor;
    public LocalClientRpc(ClientServerRpc localServer, RetryPolicy retryPolicy, Executor executor) {
        this.localServer = localServer;
        DestinationSelector<URI> uriSelector = uriSet -> localServer.serverUri();
        this.executor = executor;
        this.completableRetry = new CompletableRetry<>(retryPolicy, uriSelector);

    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return completableRetry.retry(uri -> invoke.invoke(localServer), checkRetry, executor);
    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(URI uri, CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        if(localServer.serverUri().equals(uri))  {
            return invokeClientLeaderRpc(invoke);
        } else {
            throw new IllegalArgumentException(
                    String.format("Request uri %s is NOT accessible!" +
                            "You should only request to local server %s in local client mode.",
                    uri.toString(), localServer.serverUri().toString())
            );
        }
    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientLeaderRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return invokeClientServerRpc(invoke);
    }

    @Override
    public URI getPreferredServer() {
        return localServer.serverUri();
    }

    @Override
    public void setPreferredServer(URI preferredServer) {
        // do nothing
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public void updateServers(List<URI> servers) {
        // do nothing
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        localServer.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        localServer.unWatch(eventWatcher);
    }

    private static class LocalClientCheckRetry implements CheckRetry<BaseResponse> {

        @Override
        public boolean checkException(Throwable exception) {
            try {
                logger.debug("Rpc exception: {}", exception.getMessage());
                throw exception;
            } catch (ServerBusyException | NoLeaderException ignored) {
                return true;
            } catch (Throwable ignored) {}
            return false;
        }

        @Override
        public boolean checkResult(BaseResponse response) {
            switch (response.getStatusCode()) {
                case NOT_LEADER:
                case TIMEOUT:
                case SERVER_BUSY:
                    logger.warn(response.errorString());
                    return true;
                default:
                    return false;
            }
        }
    }
}
