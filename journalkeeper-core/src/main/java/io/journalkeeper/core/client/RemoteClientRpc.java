/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.client;

import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.RequestTimeoutException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.exceptions.ServerNotFoundException;
import io.journalkeeper.exceptions.TransportException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.RandomDestinationSelector;
import io.journalkeeper.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public class RemoteClientRpc implements ClientRpc {
    private static final Logger logger = LoggerFactory.getLogger(RemoteClientRpc.class);
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private URI leaderUri = null;

    private final CompletableRetry<URI> completableRetry;
    private final RandomDestinationSelector<URI> uriSelector;
    private final ClientCheckRetry clientCheckRetry = new ClientCheckRetry();
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;
    private URI preferredServer = null;
    public RemoteClientRpc(List<URI> servers, ClientServerRpcAccessPoint clientServerRpcAccessPoint, RetryPolicy retryPolicy, Executor executor, ScheduledExecutorService scheduledExecutor) {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        if(servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Argument servers can not be empty!");
        }
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        uriSelector = new RandomUriSelector(servers);
        completableRetry = new CompletableRetry<>(retryPolicy,
                uriSelector);
//        completableRetry = new CompletableRetry<>(new IncreasingRetryPolicy(new long [] {50, 100, 500, 1000, 3000, 10000, 30000}, 50),
//                uriSelector);
    }


    @Override
    public final  <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return completableRetry.retry((uri) -> invoke.invoke(clientServerRpcAccessPoint.getClintServerRpc(uri)), clientCheckRetry, executor, scheduledExecutor);
    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(URI uri, CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return invoke.invoke(clientServerRpcAccessPoint.getClintServerRpc(uri));
    }

    @Override
    public final  <O extends BaseResponse> CompletableFuture<O> invokeClientLeaderRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
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
    public void stop () {
        this.clientServerRpcAccessPoint.stop();
    }


    private  class ClientCheckRetry implements CheckRetry<BaseResponse> {

        @Override
        public boolean checkException(Throwable exception) {
            try {

                logger.debug("Rpc exception: {}-{}", exception.getClass().getCanonicalName(), exception.getMessage());
                throw exception;
            } catch (RequestTimeoutException | ServerBusyException | TransportException | ServerNotFoundException ne) {
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
                    logger.info("{} failed, cause: {}, Retry...", response.getClass().getName(), response.errorString());
                    return true;
                case TIMEOUT:
                case SERVER_BUSY:
                case RETRY_LATER:
                case TRANSPORT_FAILED:
                case EXCEPTION:
                    logger.info("{} failed, cause: {}, Retry...", response.getClass().getName(), response.errorString());
                    return true;
                case SUCCESS:
                    return false;
                default:
                    logger.warn("{} failed, cause: {}!", response.getClass().getName(), response.errorString());
                    return false;
            }
        }
    }

    @Override
    public URI getPreferredServer() {
        return preferredServer;
    }

    @Override
    public void setPreferredServer(URI preferredServer) {
        this.preferredServer = preferredServer;
    }

    @Override
    public void updateServers(List<URI> servers) {
        uriSelector.setAllDestinations(servers);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        completableRetry.retry(uri -> {
            clientServerRpcAccessPoint.getClintServerRpc(uri)
                    .watch(eventWatcher);
            return CompletableFuture.completedFuture(null);
        }, clientCheckRetry, executor, scheduledExecutor);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        completableRetry.retry(uri -> {
            clientServerRpcAccessPoint.getClintServerRpc(uri).unWatch(eventWatcher);
            return CompletableFuture.completedFuture(null);
        }, clientCheckRetry, executor, scheduledExecutor);
    }



    private class RandomUriSelector extends RandomDestinationSelector<URI> {

        RandomUriSelector(Collection<URI> allDestinations) {
            super(allDestinations);
        }

        @Override
        public URI select(Set<URI> usedDestinations) {
            if((null == usedDestinations || usedDestinations.isEmpty()) && null != preferredServer) {
                return preferredServer;
            } else {
                return super.select(usedDestinations);
            }
        }
    }
}
