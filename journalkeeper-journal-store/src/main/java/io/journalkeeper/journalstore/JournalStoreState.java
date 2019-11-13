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
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.RAFT_PARTITION;
import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;
import static io.journalkeeper.journalstore.JournalStoreQuery.CMD_QUERY_INDEX;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreState extends LocalState<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreState.class);
    private final static String STATE_FILE_NAME = "applied_indices";
    private RaftJournal journal;
    private AppliedIndicesFile appliedIndices;
    private ExecutorService stateQueryExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("JournalStoreQueryThread"));
    protected JournalStoreState(StateFactory<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> stateFactory) {
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
    public Long execute(byte [] entry, int partition, long lastApplied, int batchSize, Map<String, String> eventData) {
        long partitionIndex = appliedIndices.getOrDefault(partition, 0L) ;
        appliedIndices.put(partition, partitionIndex + batchSize);
        long minIndex = journal.minIndex(partition);
        long maxIndex = appliedIndices.getOrDefault(partition, 0L);
        eventData.put("partition", String.valueOf(partition));
        eventData.put("minIndex", String.valueOf(minIndex));
        eventData.put("maxIndex", String.valueOf(maxIndex));
        return partitionIndex ;
    }

    @Override
    public CompletableFuture<JournalStoreQueryResult> query(JournalStoreQuery query) {
        switch (query.getCmd()) {
            case JournalStoreQuery.CMD_QUERY_ENTRIES:
                return queryEntries(query.getPartition(), query.getIndex(), query.getSize());
            case JournalStoreQuery.CMD_QUERY_PARTITIONS:
                return queryPartitions();
            case CMD_QUERY_INDEX:
                return queryIndex(query.getPartition(), query.getTimestamp());
            default:
                return CompletableFuture.supplyAsync(() -> {
                   throw new QueryJournalStoreException(String.format("Invalid command type: %d.", query.getCmd()));
                });
        }
    }

    private CompletableFuture<JournalStoreQueryResult> queryIndex(int partition, long timestamp) {

        return CompletableFuture.supplyAsync(() -> journal.queryIndexByTimestamp(partition, timestamp), stateQueryExecutor)
                .thenApply(JournalStoreQueryResult::new)
                .exceptionally(e -> new JournalStoreQueryResult(e, CMD_QUERY_INDEX));
    }

    private CompletableFuture<JournalStoreQueryResult> queryPartitions() {
        Set<Integer> partitions = journal.getPartitions();
        partitions.removeIf(partition -> partition >= RESERVED_PARTITIONS_START);

        return CompletableFuture.completedFuture(null)
                .thenApply(ignored ->
            new JournalStoreQueryResult(
                    partitions.stream()
                    .collect(Collectors.toMap(
                            Integer::intValue,
                            partition -> new JournalStoreQueryResult.Boundary(journal.minIndex(partition), appliedIndices.getOrDefault(partition, 0L))
                    ))));
    }



    private CompletableFuture<JournalStoreQueryResult> queryEntries(int partition, long index, int size) {
        return CompletableFuture.completedFuture(null)
                .thenApply(ignored ->  {
                    long maxAppliedIndex = appliedIndices.getOrDefault(partition, 0L);
                    int safeSize;
                    if(index > maxAppliedIndex || index > journal.maxIndex(partition)) {
                        return new JournalStoreQueryResult(null, null, JournalStoreQuery.CMD_QUERY_ENTRIES, index, JournalStoreQueryResult.CODE_OVERFLOW);
                    }

                    if(index < journal.minIndex(partition)) {
                        return new JournalStoreQueryResult(null, null, JournalStoreQuery.CMD_QUERY_ENTRIES, index, JournalStoreQueryResult.CODE_UNDERFLOW);
                    }

                    if(index == journal.maxIndex(partition)) {
                        return new JournalStoreQueryResult(Collections.emptyList());
                    }

                    if(index + size >= maxAppliedIndex) {
                        safeSize = (int) (maxAppliedIndex - index);
                    } else {
                        safeSize = size;
                    }
                    return new JournalStoreQueryResult(journal.batchReadByPartition(partition, index, safeSize));
                })
                .exceptionally(e -> new JournalStoreQueryResult(e.getCause(), JournalStoreQuery.CMD_QUERY_ENTRIES));
    }
}
