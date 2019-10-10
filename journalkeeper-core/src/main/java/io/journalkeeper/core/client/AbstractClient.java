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
import io.journalkeeper.utils.retry.RandomDestinationSelector;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.TimeoutException;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public abstract class AbstractClient implements Watchable, ClusterReadyAware, ServerConfigAware {
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);
    protected final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    protected URI leaderUri = null;
    private long minRetryDelayMs = 100L;
    private long maxRetryDelayMs = 300L;
    private int maxRetries = 3;

    private final CompletableRetry<URI> completableRetry;
    private final RandomDestinationSelector<URI> uriSelector;
    private final ClientCheckRetry clientCheckRetry = new ClientCheckRetry();
    private URI preferredUri = null;
    public AbstractClient(List<URI> servers, ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        if(servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Argument servers can not be empty!");
        }
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        uriSelector = new RandomUriSelector(servers);
        completableRetry = new CompletableRetry<>(minRetryDelayMs, maxRetryDelayMs, maxRetries, uriSelector);
    }

    protected abstract Executor getExecutor();

    public AbstractClient(List<URI> servers, Properties properties) {
        this(servers, ServiceSupport.load(RpcAccessPointFactory.class).createClientServerRpcAccessPoint(properties));
    }


    protected final  <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvoke<O,  ClientServerRpc> invoke) {
        return completableRetry.retry((uri) -> invoke.invoke(clientServerRpcAccessPoint.getClintServerRpc(uri)), clientCheckRetry, getExecutor());
    }

    protected final  <O extends BaseResponse> CompletableFuture<O> invokeClientLeaderRpc(CompletableRetry.RpcInvoke<O,  ClientServerRpc> invoke) {
        return invokeClientServerRpc((rpc) -> unSetLeaderUriWhenLeaderRpcFailed(
                getCachedLeaderRpc(rpc)
                .thenCompose(invoke::invoke)
                )
        );
    }


    private <O extends BaseResponse> CompletableFuture<O> unSetLeaderUriWhenLeaderRpcFailed(CompletableFuture<O> future) {
        return future
                .exceptionally(e -> {
                    this.leaderUri = null;
                    throw new CompletionException(e);
                }).thenApply(response -> {
                    if(!response.success()) {
                        this.leaderUri = null;
                    }
                    return response;
                });
    }
    protected <R> CompletableFuture<R> update(byte[] entry, int partition, int batchSize, ResponseConfig responseConfig, Serializer<R> serializer) {
        return
                invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(entry, partition, batchSize, responseConfig)))
                .thenApply(UpdateClusterStateResponse::getResult)
                .thenApply(serializer::parse);
    }

    @Override
    public void updateServers(List<URI> servers) {
        uriSelector.setAllDestinations(servers);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
//        clientServerRpcAccessPoint.defaultClientServerRpc().watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
//        clientServerRpcAccessPoint.defaultClientServerRpc().unWatch(eventWatcher);
    }

    private CompletableFuture<ClientServerRpc> getCachedLeaderRpc(ClientServerRpc clientServerRpc) {
        CompletableFuture<URI> leaderUriFuture = new CompletableFuture<>();
        if(this.leaderUri == null) {
            GetServersResponse getServersResponse = null;
            try {
                getServersResponse = clientServerRpc.getServers().get();
            } catch (Throwable e) {
                Throwable ex = e instanceof ExecutionException ? e.getCause(): e;
                leaderUriFuture = new CompletableFuture<>();
                leaderUriFuture.completeExceptionally(ex);
            }
            if(null != getServersResponse && getServersResponse.success()) {
                ClusterConfiguration clusterConfiguration = getServersResponse.getClusterConfiguration();
                if(null != clusterConfiguration) {
                    this.leaderUri = clusterConfiguration.getLeader();
                }
            }

        }

        if(this.leaderUri != null) {
            leaderUriFuture.complete(leaderUri);
        } else if(!leaderUriFuture.isDone()){
            leaderUriFuture.completeExceptionally(new NoLeaderException());
        }
//        logger.info("Current leader in client: {}", leaderUri);
        return leaderUriFuture.thenApply(clientServerRpcAccessPoint::getClintServerRpc);
    }

    @Override
    public CompletableFuture whenClusterReady(long maxWaitMs) {

        return CompletableFuture.runAsync(() -> {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() - t0 < maxWaitMs || maxWaitMs <= 0) {
                try {
                    GetServersResponse response =
                            invokeClientLeaderRpc(ClientServerRpc::getServers).get();
                    if (response.success() && response.getClusterConfiguration() != null) {
                        URI leaderUri = response.getClusterConfiguration().getLeader();
                        if(null != leaderUri) {
                            if (leaderUri.equals(this.leaderUri)) {
                                return;
                            } else {
                                this.leaderUri = leaderUri;
                            }
                        }
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


    private  class ClientCheckRetry implements CheckRetry<BaseResponse> {

        @Override
        public boolean checkException(Throwable exception) {
            try {

                logger.warn("Rpc exception: {}", exception.getMessage());
                throw exception;
            } catch (RequestTimeoutException | ServerBusyException | TransportException ne) {
                return true;
            } catch (NoLeaderException ne) {
                leaderUri = null;
                return true;
            } catch (NotLeaderException ne) {
                leaderUri = ne.getLeader();
                return true;
            }
            catch (Throwable ignored) {}
            return false;
        }

        @Override
        public boolean checkResult(BaseResponse response) {
            switch (response.getStatusCode()) {
                case NOT_LEADER:
                    leaderUri = ((LeaderResponse) response).getLeader();
                    logger.warn(response.errorString());
                    return true;
                case TIMEOUT:
                case SERVER_BUSY:
                case TRANSPORT_FAILED:
                    logger.warn(response.errorString());
                    return true;
                case SUCCESS:
                    return false;
                default:
                    logger.warn(response.errorString());
                    return false;
            }
        }
    }

    public URI getPreferredUri() {
        return preferredUri;
    }

    public void setPreferredUri(URI preferredUri) {
        this.preferredUri = preferredUri;
    }


    private class RandomUriSelector extends RandomDestinationSelector<URI> {

        public RandomUriSelector(Collection<URI> allDestinations) {
            super(allDestinations);
        }

        @Override
        public URI select(Set<URI> usedDestinations) {
            if((null == usedDestinations || usedDestinations.isEmpty()) && null != preferredUri) {
                return preferredUri;
            } else {
                return super.select(usedDestinations);
            }
        }
    }
}
