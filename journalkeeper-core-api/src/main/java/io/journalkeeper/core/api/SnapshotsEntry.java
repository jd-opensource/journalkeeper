package io.journalkeeper.core.api;

import java.util.List;

/**
 * SnapshotEntry
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class SnapshotsEntry {

    private List<SnapshotEntry> snapshots;

    public SnapshotsEntry() {

    }

    public SnapshotsEntry(List<SnapshotEntry> snapshots) {
        this.snapshots = snapshots;
    }

    public List<SnapshotEntry> getSnapshots() {
        return snapshots;
    }
}