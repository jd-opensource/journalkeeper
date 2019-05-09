package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.state.LocalState;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreState extends LocalState<byte [], JournalStoreQuery, JournalStoreQueryResult> {
    private RaftJournal journal;
    protected JournalStoreState(StateFactory<byte [], JournalStoreQuery, JournalStoreQueryResult> stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path path, RaftJournal raftJournal, Properties properties) {
        this.journal = raftJournal;
    }

    @Override
    public Map<String, String> execute(byte [] entry, short partition, long index) {
        long minIndex = journal.minIndex(partition);
        long maxIndex = journal.maxIndex(partition);
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
        return CompletableFuture.supplyAsync(() ->
            new JournalStoreQueryResult(
                    journal.getPartitions().stream()
                    .collect(Collectors.toMap(
                            Short::intValue,
                            partition -> new JournalStoreQueryResult.Boundary(journal.minIndex(partition), journal.maxIndex(partition))
                    ))));
    }

    private CompletableFuture<JournalStoreQueryResult> queryEntries(int partition, long index, int size) {
        return CompletableFuture
                .supplyAsync(() -> journal.readByPartition((short ) partition, index, size))
                .thenApply(JournalStoreQueryResult::new);
    }
}
