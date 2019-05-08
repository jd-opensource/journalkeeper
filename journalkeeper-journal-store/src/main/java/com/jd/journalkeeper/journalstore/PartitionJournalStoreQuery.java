package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class PartitionJournalStoreQuery {

    public static final int CMQ_QUERY_ENTRIES = 0;
    public static final int CMQ_QUERY_PARTITIONS = 0;
    private final int cmd;
    private final long index;
    private final int size;


    PartitionJournalStoreQuery(int cmd, long index, int size) {
        this.cmd = cmd;
        this.index = index;
        this.size = size;
    }
    private PartitionJournalStoreQuery(int cmd) {
        this(cmd, 0, 0);
    }

    public int getCmd() {
        return cmd;
    }

    public long getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }


    public static PartitionJournalStoreQuery createQueryEntries(long index, int size) {
        return new PartitionJournalStoreQuery(CMQ_QUERY_ENTRIES, index, size);
    }

    public static PartitionJournalStoreQuery createQueryPartitions() {
        return new PartitionJournalStoreQuery(CMQ_QUERY_PARTITIONS);
    }
}
