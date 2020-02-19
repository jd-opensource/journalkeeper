/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.journalstore;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.PartitionedJournalStore;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.api.transaction.TransactionalJournalStore;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.entry.internal.ReservedPartition;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.utils.event.EventWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreClient implements PartitionedJournalStore, TransactionalJournalStore {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreClient.class);
    private final RaftClient raftClient;
    private final Serializer<Long> appendResultSerializer;
    private final Serializer<JournalStoreQuery> querySerializer;
    private final Serializer<JournalStoreQueryResult> queryResultSerializer;

    JournalStoreClient(RaftClient raftClient) {
        this.raftClient = raftClient;
        this.appendResultSerializer = new LongSerializer();
        this.querySerializer = new JournalStoreQuerySerializer();
        this.queryResultSerializer = new JournalStoreQueryResultSerializer(new DefaultJournalEntryParser());

    }

    /**
     * 初始化一个远程模式的客户端
     * @param servers 集群配置
     * @param properties 属性
     */
    public JournalStoreClient(List<URI> servers, Properties properties) {
        this(servers, new DefaultJournalEntryParser(), properties);
    }

    public JournalStoreClient(List<URI> servers, JournalEntryParser journalEntryParser, Properties properties) {
        this.appendResultSerializer = new LongSerializer();
        this.querySerializer = new JournalStoreQuerySerializer();
        this.queryResultSerializer = new JournalStoreQueryResultSerializer(journalEntryParser);

        BootStrap bootStrap = new BootStrap(
                servers,
                properties
        );
        raftClient = bootStrap.getClient();
    }

    public JournalStoreClient(List<URI> servers, JournalEntryParser journalEntryParser, ExecutorService asyncExecutor, ScheduledExecutorService scheduledExecutor, Properties properties) {
        this.appendResultSerializer = new LongSerializer();
        this.querySerializer = new JournalStoreQuerySerializer();
        this.queryResultSerializer = new JournalStoreQueryResultSerializer(journalEntryParser);
        BootStrap bootStrap = new BootStrap(
                servers,
                asyncExecutor, scheduledExecutor,
                properties
        );
        raftClient = bootStrap.getClient();
    }

    @Override
    public CompletableFuture<Long> append(UpdateRequest updateRequest, boolean includeHeader, ResponseConfig responseConfig) {
        ReservedPartition.validatePartition(updateRequest.getPartition());
        return raftClient.update(updateRequest, includeHeader, responseConfig).thenApply(appendResultSerializer::parse);
    }

    @Override
    public CompletableFuture<Void> append(TransactionId transactionId, UpdateRequest updateRequest, boolean includeHeader) {
        ReservedPartition.validatePartition(updateRequest.getPartition());
        return raftClient.update(transactionId, updateRequest, includeHeader);
    }

    @Override
    public CompletableFuture<List<Long>> append(List<UpdateRequest> updateRequests, boolean includeHeader, ResponseConfig responseConfig) {
        for (UpdateRequest updateRequest : updateRequests) {
            ReservedPartition.validatePartition(updateRequest.getPartition());
        }
        return raftClient.update(updateRequests, includeHeader, responseConfig)
                .thenApply(results -> results.stream().map(appendResultSerializer::parse).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> append(TransactionId transactionId, List<UpdateRequest> updateRequests, boolean includeHeader) {
        for (UpdateRequest updateRequest : updateRequests) {
            ReservedPartition.validatePartition(updateRequest.getPartition());
        }
        return raftClient.update(transactionId, updateRequests, includeHeader);
    }

    @Override
    public CompletableFuture<List<JournalEntry>> get(int partition, long index, int size) {
        ReservedPartition.validatePartition(partition);
        return raftClient.query(querySerializer.serialize(JournalStoreQuery.createQueryEntries(partition, index, size)))
                .thenApply(queryResultSerializer::parse)
                .thenApply(result -> {
                    if (result.getCode() == JournalStoreQueryResult.CODE_SUCCESS) {
                        return result;
                    } else if (result.getCode() == JournalStoreQueryResult.CODE_UNDERFLOW) {
                        throw new CompletionException(new IndexUnderflowException());
                    } else if (result.getCode() == JournalStoreQueryResult.CODE_OVERFLOW) {
                        throw new CompletionException(new IndexOverflowException());
                    } else {
                        throw new CompletionException(new QueryJournalStoreException("Unknown exception"));
                    }
                })
                .thenApply(JournalStoreQueryResult::getEntries);
    }

    @Override
    public CompletableFuture<Map<Integer, Long>> minIndices() {
        return raftClient.query(querySerializer.serialize(JournalStoreQuery.createQueryPartitions()))
                .thenApply(queryResultSerializer::parse)
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getMin()))
                );
    }

    @Override
    public CompletableFuture<Map<Integer, Long>> maxIndices() {
        return raftClient.query(querySerializer.serialize(JournalStoreQuery.createQueryPartitions()))
                .thenApply(queryResultSerializer::parse)
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getMax()))
                );
    }

    @Override
    public CompletableFuture<Set<Integer>> listPartitions() {
        return raftClient.query(querySerializer.serialize(JournalStoreQuery.createQueryPartitions()))
                .thenApply(queryResultSerializer::parse)
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(Map::keySet);
    }

    @Override
    public CompletableFuture<Long> queryIndex(int partition, long timestamp) {
        ReservedPartition.validatePartition(partition);
        return raftClient
                .query(querySerializer.serialize(JournalStoreQuery.createQueryIndex(partition, timestamp)))
                .thenApply(queryResultSerializer::parse)
                .thenApply(result -> result.getCode() == JournalStoreQueryResult.CODE_SUCCESS ? result.getIndex() : -1L)
                .exceptionally(e -> {
                    logger.warn("Query index exception:", e);
                    return -1L;
                });
    }

    public void waitForClusterReady(long timeoutMs) throws TimeoutException {
        raftClient.waitForClusterReady(timeoutMs);
    }

    public void waitForClusterReady() {
        raftClient.waitForClusterReady();
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        raftClient.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        raftClient.watch(eventWatcher);
    }

    @Override
    public CompletableFuture<TransactionContext> createTransaction(Map<String, String> context) {
        return raftClient.createTransaction(context);
    }

    @Override
    public CompletableFuture<Void> completeTransaction(TransactionId transactionId, boolean commitOrAbort) {
        return raftClient.completeTransaction(transactionId, commitOrAbort);
    }

    @Override
    public CompletableFuture<Collection<TransactionContext>> getOpeningTransactions() {
        return raftClient.getOpeningTransactions();
    }

}
