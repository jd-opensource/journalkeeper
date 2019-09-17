package io.journalkeeper.core.client;

import io.journalkeeper.base.Serializer;
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
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.Watchable;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.RandomDestinationSelector;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public class AbstractClient implements Watchable, ClusterReadyAware, ServerConfigAware {
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);
    protected final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    protected URI leaderUri = null;
    private long minRetryDelayMs = 100L;
    private long maxRetryDelayMs = 300L;
    private int maxRetries = 3;

    private final CompletableRetry<URI> completableRetry;
    private final RandomDestinationSelector<URI> randomDestinationSelector;
    private final ClientCheckRetry clientCheckRetry = new ClientCheckRetry();
    public AbstractClient(List<URI> servers, ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        // TODO: 考虑PullWatch不依赖某一个连接
        this.clientServerRpcAccessPoint.getClintServerRpc(servers.get(0)).watch(event -> {
            if(event.getEventType() == EventType.ON_LEADER_CHANGE) {
                this.leaderUri = URI.create(event.getEventData().get("leader"));
            }
        });
        randomDestinationSelector = new RandomDestinationSelector<>(servers);
        completableRetry = new CompletableRetry<>(minRetryDelayMs, maxRetryDelayMs, maxRetries, randomDestinationSelector);
    }

    public AbstractClient(List<URI> servers, Properties properties) {
        this(servers, ServiceSupport.load(RpcAccessPointFactory.class).createClientServerRpcAccessPoint(properties));
    }

    protected final  <O extends BaseResponse, R> CompletableFuture<O> invokeClientServerRpc(R request, CompletableRetry.RpcInvoke<O, ? super R, ClientServerRpc> invoke) {
        return completableRetry.retry(request, (r, uri) -> invoke.invoke(r, clientServerRpcAccessPoint.getClintServerRpc(uri)), clientCheckRetry);
    }

    protected final  <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvokeNoRequest<O,  ClientServerRpc> invoke) {
        return completableRetry.retry((uri) -> invoke.invoke(clientServerRpcAccessPoint.getClintServerRpc(uri)), clientCheckRetry);
    }

    protected <R> CompletableFuture<R> update(byte[] entry, int partition, int batchSize, ResponseConfig responseConfig, Serializer<R> serializer) {
        return
                invokeClientServerRpc(new UpdateClusterStateRequest(entry, partition, batchSize, responseConfig),(request, rpc) -> rpc.updateClusterState(request))
                .thenApply(UpdateClusterStateResponse::getResult)
                .thenApply(serializer::parse);
    }

    @Override
    public void updateServers(List<URI> servers) {
        randomDestinationSelector.setAllDestinations(servers);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
//        clientServerRpcAccessPoint.defaultClientServerRpc().watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
//        clientServerRpcAccessPoint.defaultClientServerRpc().unWatch(eventWatcher);
    }


    @Override
    public CompletableFuture whenClusterReady(long maxWaitMs) {

        return CompletableFuture.runAsync(() -> {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() - t0 < maxWaitMs || maxWaitMs <= 0) {
                try {
                    GetServersResponse response =
                            invokeClientServerRpc(ClientServerRpc::getServers).get();
                    if (response.success() && response.getClusterConfiguration() != null) {
                        URI leaderUri = response.getClusterConfiguration().getLeader();
                        if(null != leaderUri) {
                            response = clientServerRpcAccessPoint
                                    .getClintServerRpc(leaderUri)
                                    .getServers().get();
                            if(response.success() &&
                                    response.getClusterConfiguration() != null &&
                                    leaderUri.equals(response.getClusterConfiguration().getLeader())){
                                return ;
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

    // TODO : 当网络异常时，当Leader不对时，都需要重试
    private  class ClientCheckRetry implements CheckRetry<BaseResponse> {

        @Override
        public boolean checkException(Throwable exception) {
            try {
                throw exception instanceof CompletionException ? exception.getCause() : exception;
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
                    return true;
                case TIMEOUT:
                case SERVER_BUSY:
                case TRANSPORT_FAILED:
                    return true;
                default:
                    return false;
            }
        }
    }
}
