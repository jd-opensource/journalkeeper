package io.journalkeeper.core.api;

/**
 * @author LiYue
 * Date: 2019/11/13
 */
public class UpdateRequest<E> {
    // Entry
    private final E entry;
    // 分区
    private final int partition;
    // 批量大小
    private final int batchSize;

    public UpdateRequest(E entry, int partition, int batchSize) {
        this.entry = entry;
        this.partition = partition;
        this.batchSize = batchSize;
    }

    public UpdateRequest(E entry) {
        this(entry, RaftJournal.DEFAULT_PARTITION, 1);
    }

    public E getEntry() {
        return entry;
    }

    public int getPartition() {
        return partition;
    }

    public int getBatchSize() {
        return batchSize;
    }

}
