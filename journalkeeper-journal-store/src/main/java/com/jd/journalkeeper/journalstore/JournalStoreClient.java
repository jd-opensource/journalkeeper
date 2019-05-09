package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.PartitionedJournalStore;
import com.jd.journalkeeper.core.api.RaftClient;
import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.core.client.Client;
import com.jd.journalkeeper.core.entry.reserved.CompactJournalEntry;
import com.jd.journalkeeper.core.entry.reserved.CompactJournalEntrySerializer;
import com.jd.journalkeeper.core.entry.reserved.ScalePartitionsEntry;
import com.jd.journalkeeper.core.entry.reserved.ScalePartitionsEntrySerializer;
import com.jd.journalkeeper.utils.event.EventWatcher;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreClient implements PartitionedJournalStore {
    private final CompactJournalEntrySerializer compactJournalEntrySerializer = new CompactJournalEntrySerializer();
    private final ScalePartitionsEntrySerializer scalePartitionsEntrySerializer = new ScalePartitionsEntrySerializer();
    private final RaftClient<byte [], JournalStoreQuery, JournalStoreQueryResult> raftClient;

    public JournalStoreClient(RaftClient<byte [], JournalStoreQuery, JournalStoreQueryResult> raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public CompletableFuture<Void> append(int partition, int batchSize, byte [] entries) {
        return raftClient.update(entries, partition,
                batchSize, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<Void> append(int partition, int batchSize,
                                          byte [] entries, ResponseConfig responseConfig) {
        return raftClient.update(entries, partition,
                batchSize, responseConfig);
    }

    @Override
    public CompletableFuture<List<RaftEntry>> get(int partition, long index, int size) {
        return raftClient.query(JournalStoreQuery.createQueryEntries(partition, index, size))
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
        return raftClient.update(compactJournalEntrySerializer.serialize(new CompactJournalEntry(toIndices)),
                RaftJournal.RESERVED_PARTITION, 1, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<Void> scalePartitions(int[] partitions) {
        return raftClient.update(scalePartitionsEntrySerializer.serialize(new ScalePartitionsEntry(partitions)),
                RaftJournal.RESERVED_PARTITION, 1, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<int[]> listPartitions() {
        return raftClient.query(JournalStoreQuery.createQueryPartitions())
                .thenApply(JournalStoreQueryResult::getBoundaries)
                .thenApply(boundaries -> boundaries.keySet().stream().mapToInt(Integer::intValue).toArray());
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
