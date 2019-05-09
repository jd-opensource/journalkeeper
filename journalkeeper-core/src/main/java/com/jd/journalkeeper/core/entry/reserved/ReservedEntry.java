package com.jd.journalkeeper.core.entry.reserved;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public abstract class ReservedEntry {
    public final static int TYPE_LEADER_ANNOUNCEMENT = 0;
    public final static int TYPE_COMPACT_JOURNAL = 1;
    public final static int TYPE_SCALE_PARTITIONS = 2;
    public final static int TYPE_UPDATE_VOTERS = 3;
    public final static int TYPE_UPDATE_OBSERVERS = 3;
    private final int type;

    protected ReservedEntry(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
