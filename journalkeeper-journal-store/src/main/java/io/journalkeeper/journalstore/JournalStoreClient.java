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

import io.journalkeeper.core.api.PartitionedJournalStore;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.RaftEntry;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.utils.event.EventType;
import io.journalkeeper.utils.event.EventWatcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreClient implements PartitionedJournalStore {
    private final RaftClient<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> raftClient;

    public JournalStoreClient(RaftClient<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public CompletableFuture<Long> append(int partition, int batchSize, byte [] entries) {
        return append(partition, batchSize, entries, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<Long> append(int partition, int batchSize,
                                          byte [] entries, ResponseConfig responseConfig) {
        return raftClient.update(entries, partition,
                batchSize, responseConfig);
    }

    @Override
    public CompletableFuture<List<RaftEntry>> get(int partition, long index, int size) {
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
    public CompletableFuture<Void> compact(Map<Integer, Long> toIndices) {
        return raftClient.compact(toIndices);
    }

    @Override
    public CompletableFuture<Void> scalePartitions(int[] partitions) {
        return raftClient.scalePartitions(partitions);
    }

    @Override
    public CompletableFuture<int[]> listPartitions() {
        return raftClient.query(JournalStoreQuery.createQueryPartitions())
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.keySet().stream().mapToInt(Integer::intValue).toArray());
    }

    public void waitForLeader(long timeoutMs) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        EventWatcher watcher = event -> {if(event.getEventType() == EventType.ON_LEADER_CHANGE) latch.countDown();} ;
        watch(watcher);
        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            unWatch(watcher);
        }
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        raftClient.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        raftClient.watch(eventWatcher);
    }
}
