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

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * 客户端实现
 * @author LiYue
 * Date: 2019-03-25
 */
public class Client<E, Q, R> implements RaftClient<E, Q, R> {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private final Properties properties;
    private final Serializer<E> entrySerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<R> resultSerializer;
    private URI leaderUri = null;

    public Client(ClientServerRpcAccessPoint clientServerRpcAccessPoint, Serializer<E> entrySerializer, Serializer<Q> querySerializer,
                  Serializer<R> resultSerializer, Properties properties) {
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.properties = properties;
        this.clientServerRpcAccessPoint.defaultClientServerRpc().watch(event -> {
            if(event.getEventType() == EventType.ON_LEADER_CHANGE) {
                this.leaderUri = URI.create(event.getEventData().get("leader"));
            }
        });
    }

    @Override
    public CompletableFuture<Void> update(E entry) {
        return update(entry, RaftJournal.DEFAULT_PARTITION, 1, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<Void> update(E entry, int partition, int batchSize, ResponseConfig responseConfig) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.updateClusterState(new UpdateClusterStateRequest(entrySerializer.serialize(entry), partition, batchSize, responseConfig)))
                .thenAccept(resp -> {
                    if(!resp.success()) {

                        if(resp.getStatusCode() == StatusCode.SERVER_BUSY) {
                            throw new CompletionException(new ServerBusyException());
                        } else {
                            throw new CompletionException(new RpcException(resp));
                        }
                    }
                });
    }


    @Override
    public CompletableFuture<R> query(Q query) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.queryClusterState(new QueryStateRequest(querySerializer.serialize(query))))
                .thenApply(QueryStateResponse::getResult)
                .thenApply(resultSerializer::parse);
    }

    @Override
    public CompletableFuture<ClusterConfiguration> getServers() {
        return clientServerRpcAccessPoint.defaultClientServerRpc()
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
        clientServerRpcAccessPoint.defaultClientServerRpc().watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        clientServerRpcAccessPoint.defaultClientServerRpc().unWatch(eventWatcher);
    }

    //TODO: 根据配置选择：
    // 1. 去LEADER上直接读取 <-- 现在用的是这种
    // 2. 二步读取

    //FIXME：考虑这种情况：A，B 2个server，A认为B是leader， B认为A是leader，此时会出现死循环。

    /**
     * 去Leader上请求数据，如果返回NotLeaderException，更换Leader重试。
     *
     * @param invoke 真正去Leader要调用的方法
     * @param <T> 返回的Response
     */
    private <T extends LeaderResponse> CompletableFuture<T> invokeLeaderRpc(LeaderRpc<T, E, Q, R> invoke) {
        return getLeaderRpc()
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

    private interface LeaderRpc<T extends BaseResponse, E, Q, R> {
        CompletableFuture<T> invokeLeader(ClientServerRpc leaderRpc);
    }

    private CompletableFuture<ClientServerRpc> getLeaderRpc() {
        return this.leaderUri == null ? queryLeaderRpc() :
                CompletableFuture.supplyAsync(() ->
                        this.clientServerRpcAccessPoint.getClintServerRpc(this.leaderUri));
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


    public void stop() {
        clientServerRpcAccessPoint.stop();
    }
}
