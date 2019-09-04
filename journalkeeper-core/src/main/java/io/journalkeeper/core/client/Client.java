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
import io.journalkeeper.base.VoidSerializer;
import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.entry.reserved.CompactJournalEntry;
import io.journalkeeper.core.entry.reserved.ReservedEntriesSerializeSupport;
import io.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * 客户端实现
 * @author LiYue
 * Date: 2019-03-25
 */
public class Client<E, ER, Q, QR> implements RaftClient<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private final Serializer<E> entrySerializer;
    private final Serializer<ER> entryResultSerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<QR> resultSerializer;
    private final Config config;
    private final Executor executor;
    private URI leaderUri = null;


    public Client(ClientServerRpcAccessPoint clientServerRpcAccessPoint,
                  Serializer<E> entrySerializer,
                  Serializer<ER> entryResultSerializer,
                  Serializer<Q> querySerializer,
                  Serializer<QR> queryResultSerializer,
                  Properties properties) {
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = queryResultSerializer;
        this.config = toConfig(properties);
        this.clientServerRpcAccessPoint.defaultClientServerRpc().watch(event -> {
            if(event.getEventType() == EventType.ON_LEADER_CHANGE) {
                this.leaderUri = URI.create(event.getEventData().get("leader"));
            }
        });

        this.executor = Executors.newFixedThreadPool(config.getThreads(), new NamedThreadFactory("Client-Executors"));

    }

    @Override
    public CompletableFuture<ER> update(E entry) {
        return update(entry, RaftJournal.DEFAULT_PARTITION, 1, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<ER> update(E entry, int partition, int batchSize, ResponseConfig responseConfig) {
        return update(entrySerializer.serialize(entry), partition, batchSize, responseConfig, entryResultSerializer);
    }

    private <R> CompletableFuture<R> update(byte [] entry, int partition, int batchSize, ResponseConfig responseConfig, Serializer<R> serializer) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.updateClusterState(new UpdateClusterStateRequest(entry, partition, batchSize, responseConfig)))
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


    @Override
    public CompletableFuture<QR> query(Q query) {
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
    public CompletableFuture<ClusterConfiguration> getServers(URI uri) {
         return clientServerRpcAccessPoint.getClintServerRpc(uri)
                .getServers()
                .thenApply(GetServersResponse::getClusterConfiguration);
    }

    @Override
    public CompletableFuture<Boolean> updateVoters(List<URI> oldConfig, List<URI> newConfig) {
        return invokeLeaderRpc(
                leaderRpc -> leaderRpc.updateVoters(new UpdateVotersRequest(oldConfig, newConfig)))
                .thenApply(BaseResponse::success);
    }

    @Override
    public CompletableFuture convertRoll(URI uri, RaftServer.Roll roll) {
        return clientServerRpcAccessPoint.getClintServerRpc(uri).convertRoll(new ConvertRollRequest(roll));
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
    private <T extends LeaderResponse> CompletableFuture<T> invokeLeaderRpc(LeaderRpc<T> invoke) {
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

    private interface LeaderRpc<T extends BaseResponse> {
        CompletableFuture<T> invokeLeader(ClientServerRpc leaderRpc);
    }

    private CompletableFuture<ClientServerRpc> getLeaderRpc() {
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
    public CompletableFuture<Void> compact(Map<Integer, Long> toIndices) {
        return this.update(ReservedEntriesSerializeSupport.serialize(new CompactJournalEntry(toIndices)),
                RaftJournal.RESERVED_PARTITION, 1, ResponseConfig.REPLICATION, VoidSerializer.getInstance());
    }
    @Override
    public CompletableFuture<Void> scalePartitions(int[] partitions) {
        return this.update(ReservedEntriesSerializeSupport.serialize(new ScalePartitionsEntry(partitions)),
                RaftJournal.RESERVED_PARTITION, 1, ResponseConfig.REPLICATION, VoidSerializer.getInstance());
    }

    @Override
    public CompletableFuture<Long> serverLastApplied(URI uri) {
        return clientServerRpcAccessPoint
                .getClintServerRpc(uri)
                .lastApplied()
                .exceptionally(e -> new LastAppliedResponse(-1))
                .thenApply(LastAppliedResponse::getLastApplied)
                .thenApply(l -> l == null ? -1: l);
    }


    public void stop() {
        clientServerRpcAccessPoint.stop();
    }


    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setThreads(Integer.parseInt(
                properties.getProperty(
                        Config.THREADS_KEY,
                        String.valueOf(Config.DEFAULT_THREADS))));

        return config;
    }

    static class Config {
        final static int DEFAULT_THREADS = 8;

        final static String THREADS_KEY = "client_async_threads";

        private int threads = DEFAULT_THREADS;

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }
    }
}
