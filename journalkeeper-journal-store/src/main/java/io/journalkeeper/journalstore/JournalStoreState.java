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

import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.state.LocalState;
import io.journalkeeper.exceptions.IndexOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreState extends LocalState<byte [], JournalStoreQuery, JournalStoreQueryResult> {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreState.class);
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
    public Map<String, String> execute(byte [] entry, int partition, long lastApplied, int batchSize) {

        appliedIndices.put(partition, appliedIndices.getOrDefault(partition, 0L) + batchSize);
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
                .supplyAsync(() ->  {
                    long maxAppliedIndex = appliedIndices.get(partition);
                    int safeSize;
                    if(index >= maxAppliedIndex) {
                        throw new IndexOverflowException();
                    }

                    if(index + size >= maxAppliedIndex) {
                        safeSize = (int) (maxAppliedIndex - index);
                    } else {
                        safeSize = size;
                    }
                    return journal.readByPartition(partition, index, safeSize);
                })
                .thenApply(JournalStoreQueryResult::new)
                .exceptionally(e -> new JournalStoreQueryResult(e.getCause(), JournalStoreQuery.CMQ_QUERY_ENTRIES));
    }
}
