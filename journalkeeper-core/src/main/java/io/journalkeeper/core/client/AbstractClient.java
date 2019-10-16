package io.journalkeeper.core.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.ClusterReadyAware;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.ServerConfigAware;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.RequestTimeoutException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.exceptions.TransportException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.Watchable;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.IncreasingRetryPolicy;
import io.journalkeeper.utils.retry.RandomDestinationSelector;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public abstract class AbstractClient implements ClusterReadyAware, ServerConfigAware, Watchable {
    protected final ClientRpc clientRpc;
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);
    public AbstractClient(ClientRpc clientRpc) {
        this.clientRpc = clientRpc;
    }

    protected <R> CompletableFuture<R> update(byte[] entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig, Serializer<R> serializer) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(entry, partition, batchSize, includeHeader, responseConfig)))
                .thenApply(UpdateClusterStateResponse::getResult)
                .thenApply(serializer::parse);
    }
    protected <R> CompletableFuture<R> update(byte[] entry, int partition, int batchSize, ResponseConfig responseConfig, Serializer<R> serializer) {
        return update(entry, partition, batchSize, false, responseConfig, serializer);
    }

    @Override
    public CompletableFuture whenClusterReady(long maxWaitMs) {

        return CompletableFuture.runAsync(() -> {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() - t0 < maxWaitMs || maxWaitMs <= 0) {
                try {
                    GetServersResponse response = maxWaitMs > 0 ?
                            clientRpc.invokeClientLeaderRpc(ClientServerRpc::getServers)
                                    .get(maxWaitMs, TimeUnit.MILLISECONDS) :
                            clientRpc.invokeClientLeaderRpc(ClientServerRpc::getServers)
                                    .get();
                    if(response.success()) {
                        return;
                    }
                } catch (Exception e) {
                    logger.info("Query servers failed. Error: {}", e.getMessage());
                }
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(100L));
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }
            }
            throw new CompletionException(new TimeoutException());
        });
    }

    @Override
    public void updateServers(List<URI> servers) {
        clientRpc.updateServers(servers);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        clientRpc.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        clientRpc.unWatch(eventWatcher);
    }

    public void stop(){
        clientRpc.stop();
    }
}
