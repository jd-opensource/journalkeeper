package com.jd.journalkeeper.core.client;

import com.jd.journalkeeper.base.event.EventWatcher;
import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.core.api.JournalKeeperClient;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.client.*;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端实现
 * @author liyue25
 * Date: 2019-03-25
 */
public class Client<E, Q, R> implements JournalKeeperClient<E, Q, R> {

    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private final Properties properties;

    public Client(ClientServerRpcAccessPoint clientServerRpcAccessPoint, Properties properties) {
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        this.properties = properties;
    }

    @Override
    public CompletableFuture<Void> update(E entry) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.updateClusterState(new UpdateClusterStateRequest<>(entry)))
                .thenAccept(resp -> {});
    }


    @Override
    public CompletableFuture<R> query(Q query) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.queryClusterState(new QueryStateRequest<>(query)))
                .thenApply(QueryStateResponse::getResult);
    }

    @Override
    public CompletableFuture<ClusterConfiguration> getServers() {
        return clientServerRpcAccessPoint.getClintServerRpc()
                .getServers()
                .thenApply(GetServersResponse::getClusterConfiguration);
    }

    @Override
    public CompletableFuture<Boolean> updateVoters(UpdateVoterOperation operation, URI voter) {
        // TODO 变更集群配置
        return null;
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        // TODO 事件通知
    }

    @Override
    public void unwatch(EventWatcher eventWatcher) {

    }

    //TODO: 根据配置选择：
    // 1. 去LEADER上直接读取 <-- 现在用的是这种
    // 2. 二步读取

    //FIXME：考虑这种情况：A，B2个server，A认为B是leader， B认为A是leader，此时会出现死循环。

    /**
     * 去Leader上请求数据，如果返回NotLeaderException，更换Leader重试。
     *
     * @param invoke 真正去Leader要调用的方法
     * @param <T> 返回的Response
     */
    private <T extends BaseResponse> CompletableFuture<T> invokeLeaderRpc(LeaderRpc<T, E, Q, R> invoke) {
        return getLeaderRpc()
                .thenCompose(invoke::invokeLeader)
                .thenCompose(resp -> {
                    try {
                        if (resp.getException() != null) {
                            throw resp.getException();
                        }
                    } catch (NotLeaderException nle) {
                        return invoke.invokeLeader(clientServerRpcAccessPoint.getClintServerRpc(nle.getLeader()));
                    } catch (Throwable ignored) {}
                    return CompletableFuture.supplyAsync(() -> resp);
                });
    }

    private interface LeaderRpc<T extends BaseResponse, E, Q, R> {
        CompletableFuture<T> invokeLeader(ClientServerRpc<E, Q, R> leaderRpc);
    }

    private CompletableFuture<ClientServerRpc<E, Q, R>> getLeaderRpc() {
        return clientServerRpcAccessPoint
                .getClintServerRpc()
                .getServers()
                .thenApply(resp ->
                        clientServerRpcAccessPoint.getClintServerRpc(
                                resp.getClusterConfiguration().getLeader()));
    }

}
