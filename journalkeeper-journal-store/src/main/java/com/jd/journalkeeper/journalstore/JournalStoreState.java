package com.jd.journalkeeper.journalstore;


import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.exception.StateExecutionException;
import com.jd.journalkeeper.core.exception.StateRecoverException;
import com.jd.journalkeeper.core.server.Server;
import com.jd.journalkeeper.core.state.LocalState;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * State中存放Journal Index 和 Raft Index的对应关系。
 * 由于Raft本身选举复制需要在Raft Journal中插入一些Entry，导致Raft Index和Journal Index不能一一对应。
 * 需要保存二者映射关系，以便于读取Journal时找到对应的Entry。
 *
 *                                   Raft Entries
 *                                     +
 *                            +--------------------------+
 *                            |        |                 |
 *                            v        v                 v
 *               +------------+--------+-----------------+----------+
 * Journal Index | 0| 1| 2| 3| R| 4| 5| R| 6| 7| 8| 9|10| R|11|12|13|
 *               +--------------------------------------------------+
 *    Raft Index | 0| 1| 2| 3| 4| 5| 6| 7| 8| 9|10|11|12|13|14|15|16|
 *               +----------------+--------+-----------------+------+
 *                ^               ^        ^                 ^
 *                |               |        |                 |
 *                +               +        +                 +
 *    Index Map <0,0>           <4,5>    <6,8>            <11,14>
 *
 * 我们采用一个TreeMap存放对应关系，只存放每个Raft Entry下一个位置的对应关系，
 * Key存放Journal Index， Value存放Raft Index。
 *
 * 例如我们要查找索引序号为8的Entry eq：
 *
 * 1. 先在IndexMap中查找不大于8的最大的元素：ei = indexMap.floorEntry(8) -> <6, 8>;
 * 2. 由于找到的Entry ei 与待查找的找到的Entry eq之间不会有Raft Entry;
 * 3. 因此Entry ei的Journal Index和Raft Index的差值与待查找的Entry是一样的；
 * 4. 待查找Entry的Raft Index：eq.raftIndex = eq.journalIndex + ei.raftIndex - ei.journalIndex 即 8 + 8 - 6 = 10。
 *
 * 特殊情况：
 *
 * 1. 位置0如果存放的不是RaftIndex，也需要记录到IndexMap中；
 * 2. 特殊处理一下Journal尾部是一个Raft Entry的情况；
 *
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalStoreState extends LocalState<byte[], JournalStoreQuery, List<byte[]>> {
    private final static String FILENAME = "index.map";
    private NavigableMap<Long, Long> indexMap;
    private long nextJournalIndex = 0L;
    private RaftJournal raftJournal = null;
    private int skipped = 0;
    private final Object fileWriteMutex = new Object();
    private File file = null;
    private final static int COMPACT_THRESHOLD = 1024 * 1024;
    /**
     * 最后一条Entry不是Journal Entry：意味着，执行下一条Entry时要在indexMap中添加一条记录。
     */
    private boolean lastEntryNotAJournalEntry = false;
    JournalStoreState(StateFactory<byte[], JournalStoreQuery, List<byte[]>> stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path path, RaftJournal raftJournal, Properties properties) {
        try {
            this.file = path.resolve(FILENAME).toFile();
            indexMap = recoverIndexMap(file);
            skipped = IndexMapPersistence.skipped(file);
            // 最后一条Entry不是Journal Entry
            lastEntryNotAJournalEntry = lastApplied() - 1 < raftJournal.minIndex() || // 没有数据
                    !isStateEntry(raftJournal, lastApplied() - 1);
            if(indexMap.isEmpty()) {
                indexMap.put(lastApplied(), lastApplied());
            }
            nextJournalIndex = lastApplied() + indexMap.lastKey() - indexMap.lastEntry().getValue();
            this.raftJournal = raftJournal;
        } catch (IOException e) {
            throw new StateRecoverException(e);
        }
    }

    private boolean isStateEntry(RaftJournal journal, long index) {
        return journal.readEntryHeader(index).getPartition() == Server.DEFAULT_STATE_PARTITION;
    }

    private NavigableMap<Long, Long> recoverIndexMap(File file) throws IOException {
        NavigableMap<Long,Long> recoverMap = new TreeMap<>();

        IndexMapPersistence.restore(recoverMap, file);
        return recoverMap;
    }

    public long getRaftIndex(long journalIndex) {
        Map.Entry<Long, Long> floorEntry = indexMap.floorEntry(journalIndex);
        if(floorEntry != null) {
            return journalIndex + floorEntry.getKey() - floorEntry.getValue();
        } else {
            return journalIndex;
        }
    }

    @Override
    public Map<String, String> execute(byte[] entry, long raftIndex) {
        try {
            if (lastEntryNotAJournalEntry) {
                synchronized (fileWriteMutex) {
                    IndexMapPersistence.add(nextJournalIndex, raftIndex, file);
                    indexMap.put(nextJournalIndex, raftIndex);
                }
            }
            nextJournalIndex++;
            long minIndex = indexMap.firstKey();
            long maxIndex = nextJournalIndex;
            Map<String, String> eventData = new HashMap<>(2);
            eventData.put("minIndex", String.valueOf(minIndex));
            eventData.put("maxIndex", String.valueOf(maxIndex));
            return eventData;
        } catch (Throwable t) {
            throw new StateExecutionException(t);
        }

    }

    @Override
    protected void flushState(Path statePath) throws IOException {
        super.flushState(statePath);
        if(skipped > COMPACT_THRESHOLD && skipped > indexMap.size()) {
            synchronized (fileWriteMutex) {
                IndexMapPersistence.compact(statePath.resolve(FILENAME).toFile());
                skipped = 0;
            }
        }
    }

    @Override
    public void skip() {
        lastEntryNotAJournalEntry = true;
    }

    @Override
    public CompletableFuture<List<byte[]>> query(JournalStoreQuery query) {
        return CompletableFuture
                .supplyAsync(() -> raftJournal.batchRead(getRaftIndex(query.getIndex()), query.getSize()))
                .thenApply(raftEntries -> raftEntries.stream().map(RaftEntry::getEntry).collect(Collectors.toList()));
    }


    void compact(long indexExclusive) throws IOException {
        SortedMap<Long, Long> compactMap = indexMap.headMap(indexExclusive);
        if(!compactMap.isEmpty()) {
            long raftIndex = getRaftIndex(indexExclusive);
            synchronized (fileWriteMutex) {
                int deleteSize = compactMap.size();
                if(null == indexMap.putIfAbsent(indexExclusive, raftIndex)) {
                    deleteSize --;
                    IndexMapPersistence.update(indexExclusive, raftIndex, skipped + deleteSize, file);
                }
                IndexMapPersistence.delete(deleteSize, file);
                skipped += deleteSize;
                compactMap.clear();


            }
        }
    }

    long minIndex() {
        return indexMap.firstKey();
    }

    long maxIndex() {
        Map.Entry<Long, Long> last = indexMap.lastEntry();
        return lastApplied() - last.getValue() + last.getKey();
    }
}
