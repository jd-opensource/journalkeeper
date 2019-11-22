package io.journalkeeper.core.journal;

import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/22
 */
public interface JournalSnapshot {
    long minIndex();
    long minOffset();
    Map<Integer /* partition */ , Long /* min index of the partition */ > partitionMinIndices();
}
