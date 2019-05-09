package com.jd.journalkeeper.core.entry.reserved;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class ScalePartitionsEntry extends ReservedEntry {
    private final int [] partitions;
    public ScalePartitionsEntry(int[] partitions) {
        super(TYPE_SCALE_PARTITIONS);
        this.partitions = partitions;
    }

    public int[] getPartitions() {
        return partitions;
    }
}
