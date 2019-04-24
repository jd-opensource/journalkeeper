package com.jd.journalkeeper.core.api;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public interface RaftJournal {
    long minIndex();

    long maxIndex();

    byte [] read(long index);

    List<byte []> read(long index, int size);

    boolean isStateEntry(long index);
}
