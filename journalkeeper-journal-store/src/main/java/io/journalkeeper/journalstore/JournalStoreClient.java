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
package io.journalkeeper.journalstore;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.*;
import io.journalkeeper.core.api.transaction.TransactionalJournalStore;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.entry.reserved.ReservedPartition;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.utils.event.EventWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreClient implements PartitionedJournalStore, TransactionalJournalStore {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreClient.class);
    private final RaftClient<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> raftClient;

    JournalStoreClient(RaftClient<byte[], Long, JournalStoreQuery, JournalStoreQueryResult> raftClient) {
        this.raftClient = raftClient;
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
        BootStrap<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> bootStrap = new BootStrap<>(
                servers,
                new ByteArraySerializer(),
                new LongSerializer(),
                new JournalStoreQuerySerializer(),
                new JournalStoreQueryResultSerializer(journalEntryParser),
                properties
                );
        raftClient = bootStrap.getClient();
    }

    @Override
    public CompletableFuture<Long> append(int partition, int batchSize,
                                          byte [] entries, boolean includeHeader, ResponseConfig responseConfig) {
        ReservedPartition.validatePartition(partition);
        return raftClient.update(entries, partition,
                batchSize, includeHeader, responseConfig);
    }

    @Override
    public CompletableFuture<List<JournalEntry>> get(int partition, long index, int size) {
        ReservedPartition.validatePartition(partition);
        return raftClient.query(JournalStoreQuery.createQueryEntries(partition, index, size))
                .thenApply(result -> {
                    if(result.getCode() == JournalStoreQueryResult.CODE_SUCCESS) {
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
        return raftClient.query(JournalStoreQuery.createQueryPartitions())
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getMin()))
                );
    }

    @Override
    public CompletableFuture<Map<Integer, Long>> maxIndices() {
        return raftClient.query(JournalStoreQuery.createQueryPartitions())
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getMax()))
                );
    }

    @Override
    public CompletableFuture<int[]> listPartitions() {
        return raftClient.query(JournalStoreQuery.createQueryPartitions())
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.keySet().stream().mapToInt(Integer::intValue).toArray());
    }

    @Override
    public CompletableFuture<Long> queryIndex(int partition, long timestamp) {
        ReservedPartition.validatePartition(partition);
        return raftClient
                .query(JournalStoreQuery.createQueryIndex(partition, timestamp))
                .thenApply(result -> result.getCode() == JournalStoreQueryResult.CODE_SUCCESS ? result.getIndex(): -1L)
                .exceptionally(e -> {
                    logger.warn("Query index exception:", e);
                    return -1L;
                });
    }

    public void waitForClusterReady(long timeoutMs) throws InterruptedException, ExecutionException {
        raftClient.waitForClusterReady(timeoutMs);
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
    public CompletableFuture<UUID> createTransaction() {
        return raftClient.createTransaction();
    }

    @Override
    public CompletableFuture<Void> completeTransaction(UUID transactionId, boolean commitOrAbort) {
        return raftClient.completeTransaction(transactionId, commitOrAbort);
    }

    @Override
    public CompletableFuture<Collection<UUID>> getOpeningTransactions() {
        return raftClient.getOpeningTransactions();
    }

    @Override
    public CompletableFuture<Void> append(UUID transactionId, byte[] entry, int partition, int batchSize, boolean includeHeader) {
        return raftClient.update(transactionId, entry, partition, batchSize, includeHeader);
    }
}
