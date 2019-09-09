package io.journalkeeper.core.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.ClusterReadyAware;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.ServerConfigAware;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.Watchable;
import io.journalkeeper.utils.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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

    public AbstractClient(ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        this.clientServerRpcAccessPoint.defaultClientServerRpc().watch(event -> {
            if(event.getEventType() == EventType.ON_LEADER_CHANGE) {
                this.leaderUri = URI.create(event.getEventData().get("leader"));
            }
        });
    }

    public AbstractClient(List<URI> servers, Properties properties) {
        RpcAccessPointFactory rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(servers, properties);
    }

    protected <R> CompletableFuture<R> update(byte[] entry, int partition, int batchSize, ResponseConfig responseConfig, Serializer<R> serializer, Executor executor) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.updateClusterState(new UpdateClusterStateRequest(entry, partition, batchSize, responseConfig)), executor)
                .thenApply(resp -> {
                    if(!resp.success()) {
//                        logger.warn("Response failed: {}", resp.errorString());
                        if(resp.getStatusCode() == StatusCode.SERVER_BUSY) {
                            throw new CompletionException(new ServerBusyException());
                        } else if (resp.getStatusCode() == StatusCode.TIMEOUT) {
                            throw new CompletionException(new TimeoutException());
                        } else {
                            throw new CompletionException(new RpcException(resp));
                        }

                    } else {
                        return resp.getResult();
                    }

                })
                .thenApply(serializer::parse);
    }

    /**
     * 去Leader上请求数据，如果返回NotLeaderException，更换Leader重试。
     *
     * @param invoke 真正去Leader要调用的方法
     * @param <T> 返回的Response
     */
    protected <T extends LeaderResponse> CompletableFuture<T> invokeLeaderRpc(LeaderRpc<T> invoke, Executor executor) {
        return getLeaderRpc(executor)
                .thenCompose(invoke::invokeLeader)
                .thenCompose(resp -> {
                    if (resp.getStatusCode() == StatusCode.NOT_LEADER) {
                        this.leaderUri = resp.getLeader();
                        return invoke.invokeLeader(clientServerRpcAccessPoint.getClintServerRpc(resp.getLeader()));
                    } else {
                        return CompletableFuture.supplyAsync(() -> resp);
                    }
                });
    }

    private CompletableFuture<ClientServerRpc> getLeaderRpc(Executor executor) {
        return this.leaderUri == null ? queryLeaderRpc() :
                CompletableFuture.supplyAsync(() ->
                        this.clientServerRpcAccessPoint.getClintServerRpc(this.leaderUri), executor);
    }

    private CompletableFuture<ClientServerRpc> queryLeaderRpc() {
        return clientServerRpcAccessPoint
        .defaultClientServerRpc()
        .getServers()
        .exceptionally(GetServersResponse::new)
        .thenApplyAsync(resp -> {
            if(resp.success()) {
                if(resp.getClusterConfiguration() != null && resp.getClusterConfiguration().getLeader() != null) {
                    return clientServerRpcAccessPoint.getClintServerRpc(
                            resp.getClusterConfiguration().getLeader());
                } else {
                    throw new NoLeaderException();
                }
            } else {
                 throw new RpcException(resp);
            }
        });
    }

    @Override
    public void updateServers(List<URI> servers) {
        clientServerRpcAccessPoint.updateServers(servers);
    }

    public interface LeaderRpc<T extends BaseResponse> {
        CompletableFuture<T> invokeLeader(ClientServerRpc leaderRpc);
    }


    @Override
    public void watch(EventWatcher eventWatcher) {
        clientServerRpcAccessPoint.defaultClientServerRpc().watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        clientServerRpcAccessPoint.defaultClientServerRpc().unWatch(eventWatcher);
    }


    @Override
    public CompletableFuture waitClusterReady(long maxWaitMs) {

        return CompletableFuture.runAsync(() -> {
            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() - t0 < maxWaitMs || maxWaitMs <= 0) {
                try {
                    GetServersResponse response = clientServerRpcAccessPoint
                            .defaultClientServerRpc()
                            .getServers().get();
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
}
