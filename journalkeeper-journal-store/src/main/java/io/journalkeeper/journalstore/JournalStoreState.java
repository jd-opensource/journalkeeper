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

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.journalkeeper.core.api.RaftJournal.RESERVED_PARTITIONS_START;
import static io.journalkeeper.journalstore.JournalStoreQuery.CMD_QUERY_ENTRIES;
import static io.journalkeeper.journalstore.JournalStoreQuery.CMD_QUERY_INDEX;
import static io.journalkeeper.journalstore.JournalStoreQuery.CMD_QUERY_PARTITIONS;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreState implements State , Flushable {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreState.class);
    private final static String STATE_FILE_NAME = "applied_indices";
    private AppliedIndicesFile appliedIndices;
    private Path path;
    private final Serializer<Long> appendResultSerializer;
    private final Serializer<JournalStoreQuery> querySerializer;
    private final Serializer<JournalStoreQueryResult> queryResultSerializer;

    JournalStoreState(JournalEntryParser journalEntryParser) {
        this.appendResultSerializer = new LongSerializer();
        this.querySerializer = new JournalStoreQuerySerializer();
        this.queryResultSerializer = new JournalStoreQueryResultSerializer(journalEntryParser);
    }


    @Override
    public void recover(Path path, Properties properties) throws IOException{
        this.path = path;
        appliedIndices = recoverAppliedIndices(path.resolve(STATE_FILE_NAME));
    }

    @Override
    public void flush() {
        if(null != path) {
            flushAppliedIndices(path.resolve(STATE_FILE_NAME));
        }
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
    public StateResult execute(byte [] entry, int partition, long lastApplied, int batchSize, RaftJournal journal) {
        long partitionIndex = appliedIndices.getOrDefault(partition, 0L) ;
        appliedIndices.put(partition, partitionIndex + batchSize);
        long minIndex = journal.minIndex(partition);
        long maxIndex = appliedIndices.getOrDefault(partition, 0L);
        StateResult result = new StateResult(appendResultSerializer.serialize(partitionIndex));
        Map<String, String> eventData = result.getEventData();
        eventData.put("partition", String.valueOf(partition));
        eventData.put("minIndex", String.valueOf(minIndex));
        eventData.put("maxIndex", String.valueOf(maxIndex));
        return result ;
    }

    @Override
    public byte[] query(byte[] query, RaftJournal journal) {
        return queryResultSerializer.serialize(query(querySerializer.parse(query), journal));
    }
     private JournalStoreQueryResult query(JournalStoreQuery query, RaftJournal journal) {
        try {
            switch (query.getCmd()) {
                case CMD_QUERY_ENTRIES:
                    return queryEntries(query.getPartition(), query.getIndex(), query.getSize(), journal);
                case CMD_QUERY_PARTITIONS:
                    return queryPartitions(journal);
                case CMD_QUERY_INDEX:
                    return queryIndex(query.getPartition(), query.getTimestamp(), journal);
                default:
                    throw new QueryJournalStoreException(String.format("Invalid command type: %d.", query.getCmd()));

            }
        } catch (Throwable e) {
            return new JournalStoreQueryResult(e, query.getCmd());
        }
    }

    private JournalStoreQueryResult queryIndex(int partition, long timestamp, RaftJournal journal) {

        return  new JournalStoreQueryResult(journal.queryIndexByTimestamp(partition, timestamp));
    }

    private JournalStoreQueryResult queryPartitions(RaftJournal journal) {
        Set<Integer> partitions = journal.getPartitions();
        partitions.removeIf(partition -> partition >= RESERVED_PARTITIONS_START);
        return
            new JournalStoreQueryResult(
                    partitions.stream()
                    .collect(Collectors.toMap(
                            Integer::intValue,
                            partition -> new JournalStoreQueryResult.Boundary(journal.minIndex(partition), appliedIndices.getOrDefault(partition, 0L))
                    )));
    }



    private JournalStoreQueryResult queryEntries(int partition, long index, int size, RaftJournal journal) {
        long maxAppliedIndex = appliedIndices.getOrDefault(partition, 0L);
        int safeSize;
        if(index > maxAppliedIndex || index > journal.maxIndex(partition)) {
            return new JournalStoreQueryResult(null, null, CMD_QUERY_ENTRIES, index, JournalStoreQueryResult.CODE_OVERFLOW);
        }

        if(index < journal.minIndex(partition)) {
            return new JournalStoreQueryResult(null, null, CMD_QUERY_ENTRIES, index, JournalStoreQueryResult.CODE_UNDERFLOW);
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

    }

    @Override
    public void close() {
        if(null != appliedIndices) {
            appliedIndices.close();
        }
    }
}
