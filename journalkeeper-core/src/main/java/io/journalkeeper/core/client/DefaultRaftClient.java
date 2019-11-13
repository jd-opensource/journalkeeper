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
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.client.*;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 客户端实现
 * @author LiYue
 * Date: 2019-03-25
 */
public class DefaultRaftClient<E, ER, Q, QR> extends AbstractClient implements RaftClient<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRaftClient.class);
    private final Serializer<E> entrySerializer;
    private final Serializer<ER> entryResultSerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<QR> resultSerializer;
    private final Config config;
    private final Executor executor;

    public DefaultRaftClient(ClientRpc clientRpc,
                             Serializer<E> entrySerializer,
                             Serializer<ER> entryResultSerializer,
                             Serializer<Q> querySerializer,
                             Serializer<QR> queryResultSerializer,
                             Properties properties) {
        super(clientRpc);
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = queryResultSerializer;
        this.config = toConfig(properties);
        this.executor = Executors.newFixedThreadPool(config.getThreads(), new NamedThreadFactory("Client-Executors"));

    }

    @Override
    public CompletableFuture<ER> update(E entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig) {
        return update(entrySerializer.serialize(entry), partition, batchSize, includeHeader, responseConfig, entryResultSerializer);
    }


    @Override
    public CompletableFuture<QR> query(Q query) {
        return clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.queryClusterState(new QueryStateRequest(querySerializer.serialize(query))))
                .thenApply(super::checkResponse)
                .thenApply(QueryStateResponse::getResult)
                .thenApply(resultSerializer::parse);
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();
        config.setThreads(Integer.parseInt(
                properties.getProperty(
                        Config.THREADS_KEY,
                        String.valueOf(Config.DEFAULT_THREADS))));

        return config;
    }

    @Override
    public CompletableFuture<UUID> createTransaction() {
        return clientRpc.invokeClientLeaderRpc(ClientServerRpc::createTransaction)
                .thenApply(super::checkResponse)
                .thenApply(CreateTransactionResponse::getTransactionId);
    }

    @Override
    public CompletableFuture<Void> completeTransaction(UUID transactionId, boolean commitOrAbort) {
        return clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.completeTransaction(
                new CompleteTransactionRequest(transactionId, commitOrAbort)))
                .thenApply(super::checkResponse)
                .thenApply(response -> null);
    }

    @Override
    public CompletableFuture<Collection<UUID>> getOpeningTransactions() {
        return clientRpc.invokeClientLeaderRpc(ClientServerRpc::getOpeningTransactions)
                .thenApply(super::checkResponse)
                .thenApply(GetOpeningTransactionsResponse::getTransactionIds);
    }

    @Override
    public CompletableFuture<Void> update(UUID transactionId, byte[] entry, int partition, int batchSize, boolean includeHeader) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(transactionId, entry, partition, batchSize, includeHeader)))
                        .thenApply(this::checkResponse)
                        .thenApply(response -> null);
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
