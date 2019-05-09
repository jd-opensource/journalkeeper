package com.jd.journalkeeper.journalstore;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class JournalStoreQuery {

    public static final int CMQ_QUERY_ENTRIES = 0;
    public static final int CMQ_QUERY_PARTITIONS = 1;
    private final int cmd;
    private final int partition;
    private final long index;
    private final int size;


    JournalStoreQuery(int cmd, int partition, long index, int size) {
        this.cmd = cmd;
        this.partition = partition;
        this.index = index;
        this.size = size;
    }
    private JournalStoreQuery(int cmd) {
        this(cmd, 0, 0, 0);
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

    public int getPartition() {
        return partition;
    }

    public static JournalStoreQuery createQueryEntries(int partition, long index, int size) {
        return new JournalStoreQuery(CMQ_QUERY_ENTRIES, partition, index, size);
    }

    public static JournalStoreQuery createQueryPartitions() {
        return new JournalStoreQuery(CMQ_QUERY_PARTITIONS);
    }
}
