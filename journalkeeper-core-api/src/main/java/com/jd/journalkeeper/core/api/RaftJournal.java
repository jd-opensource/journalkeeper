package com.jd.journalkeeper.core.api;

import java.util.List;
import java.util.Set;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public interface RaftJournal {
    short DEFAULT_PARTITION = 0;
    short RESERVED_PARTITION = Short.MAX_VALUE;
    long minIndex();

    long maxIndex();

    long minIndex(short partition);

    long maxIndex(short partition);

    RaftEntry readByPartition(short partition, long index);

    List<RaftEntry> readByPartition(short partition, long index, int maxSize);

    RaftEntry read(long index);

    List<RaftEntry> batchRead(long index, int size);

    RaftEntryHeader readEntryHeader(long index);

    Set<Short> getPartitions();
}
