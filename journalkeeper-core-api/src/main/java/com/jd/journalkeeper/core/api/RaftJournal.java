package com.jd.journalkeeper.core.api;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public interface RaftJournal {
    long minIndex();

    long maxIndex();

    RaftEntry read(long index);

    List<RaftEntry> batchRead(long index, int size);

    RaftEntryHeader readEntryHeader(long index);
}
