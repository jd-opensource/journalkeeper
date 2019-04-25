package com.jd.journalkeeper.core.journal;

/**
 * @author liyue25
 * Date: 2019-04-25
 */
public class NosuchPartitionException extends RuntimeException {
    private final short partition;
    public NosuchPartitionException(short partition) {
        super("No such partition: " + partition + "!");
        this.partition = partition;
    }

    public short getPartition() {
        return partition;
    }
}
