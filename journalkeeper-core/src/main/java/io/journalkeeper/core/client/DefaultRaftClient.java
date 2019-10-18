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
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
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
                .thenApply(response -> {
                    if(response.getStatusCode() == StatusCode.SUCCESS) {
                        return response.getResult();
                    } else {
                        throw new RpcException(response);
                    }
                })
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
