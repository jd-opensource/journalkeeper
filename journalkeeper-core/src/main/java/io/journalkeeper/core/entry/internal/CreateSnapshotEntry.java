package io.journalkeeper.core.entry.internal;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public class CreateSnapshotEntry extends InternalEntry {
    public CreateSnapshotEntry() {
        super(InternalEntryType.TYPE_CREATE_SNAPSHOT);
    }
}
