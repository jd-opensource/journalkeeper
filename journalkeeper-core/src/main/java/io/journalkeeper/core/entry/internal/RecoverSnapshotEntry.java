package io.journalkeeper.core.entry.internal;

/**
 * RecoverSnapshotEntry
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotEntry extends InternalEntry {

    private long index;

    public RecoverSnapshotEntry() {
        super(InternalEntryType.TYPE_RECOVER_SNAPSHOT);
    }

    public RecoverSnapshotEntry(long index) {
        super(InternalEntryType.TYPE_RECOVER_SNAPSHOT);
        this.index = index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getIndex() {
        return index;
    }
}