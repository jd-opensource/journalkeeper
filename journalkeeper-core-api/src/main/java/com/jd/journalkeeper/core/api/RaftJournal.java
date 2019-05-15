package com.jd.journalkeeper.core.api;

import java.util.List;
import java.util.Set;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public interface RaftJournal {
    int DEFAULT_PARTITION = 0;
    int RESERVED_PARTITION = Short.MAX_VALUE;
    long minIndex();

    long maxIndex();

    long minIndex(int partition);

    long maxIndex(int partition);

    RaftEntry readByPartition(int partition, long index);

    List<RaftEntry> readByPartition(int partition, long index, int maxSize);

    RaftEntry read(long index);

    List<RaftEntry> batchRead(long index, int size);

    RaftEntryHeader readEntryHeader(long index);

    Set<Integer> getPartitions();
}
