package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.state.LocalState;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreState extends LocalState<byte [], JournalStoreQuery, JournalStoreQueryResult> {
    private final static String STATE_FILE_NAME = "applied_indices";
    private RaftJournal journal;
    private AppliedIndicesFile appliedIndices;

    protected JournalStoreState(StateFactory<byte [], JournalStoreQuery, JournalStoreQueryResult> stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path path, RaftJournal raftJournal, Properties properties) throws IOException{
        this.journal = raftJournal;
        appliedIndices = recoverAppliedIndices(path.resolve(STATE_FILE_NAME));
    }

    @Override
    protected void flushState(Path statePath) throws IOException {
        super.flushState(statePath);
        flushAppliedIndices(statePath.resolve(STATE_FILE_NAME));
    }

    private void flushAppliedIndices(Path path) {
        appliedIndices.flush();
    }

    /**
     * 从文件恢复
     */
    private AppliedIndicesFile recoverAppliedIndices(Path path) throws IOException {
        AppliedIndicesFile appliedIndicesFile = new AppliedIndicesFile(path.toFile());
        appliedIndicesFile.recover();
        return appliedIndicesFile;
    }


    @Override
    public Map<String, String> execute(byte [] entry, int partition, long lastApplied) {

        appliedIndices.put(partition, appliedIndices.getOrDefault(partition, 0L) + 1);

        long minIndex = journal.minIndex(partition);
        long maxIndex = appliedIndices.get(partition);
        Map<String, String> eventData = new HashMap<>(3);
        eventData.put("partition", String.valueOf(partition));
        eventData.put("minIndex", String.valueOf(minIndex));
        eventData.put("maxIndex", String.valueOf(maxIndex));
        return eventData;
    }

    @Override
    public CompletableFuture<JournalStoreQueryResult> query(JournalStoreQuery query) {
        switch (query.getCmd()) {
            case JournalStoreQuery.CMQ_QUERY_ENTRIES:
                return queryEntries(query.getPartition(), query.getIndex(), query.getSize());
            case JournalStoreQuery.CMQ_QUERY_PARTITIONS:
                return queryPartitions();
            default:
                return CompletableFuture.supplyAsync(() -> {
                   throw new QueryJournalStoreException(String.format("Invalid command type: %d.", query.getCmd()));
                });
        }
    }

    private CompletableFuture<JournalStoreQueryResult> queryPartitions() {
        Set<Integer> partitions = journal.getPartitions();
        removePartitionsNotExistsAsync(partitions);
        return CompletableFuture.supplyAsync(() ->
            new JournalStoreQueryResult(
                    partitions.stream()
                    .collect(Collectors.toMap(
                            Integer::intValue,
                            partition -> new JournalStoreQueryResult.Boundary(journal.minIndex(partition), appliedIndices.get(partition))
                    ))));
    }

    private void removePartitionsNotExistsAsync(Set<Integer> partitions) {
        CompletableFuture.runAsync(() -> {
            Set<Integer> tobeRemoved = appliedIndices.keySet().stream()
                    .filter(k -> !partitions.contains(k)).collect(Collectors.toSet());
            tobeRemoved.forEach(appliedIndices::remove);
        });
    }

    private CompletableFuture<JournalStoreQueryResult> queryEntries(int partition, long index, int size) {
        return CompletableFuture
                .supplyAsync(() -> journal.readByPartition(partition, index, size))
                .thenApply(JournalStoreQueryResult::new);
    }
}
