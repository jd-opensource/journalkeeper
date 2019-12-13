package io.journalkeeper.core.api;

/**
 * SnapshotEntry
 * author: gaohaoxiang
 * date: 2019/12/13
 */
public class SnapshotEntry {

    private String path;
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private long minOffset;
    private long timestamp;

    public SnapshotEntry() {

    }

    public SnapshotEntry(String path, long lastIncludedIndex, int lastIncludedTerm, long minOffset, long timestamp) {
        this.path = path;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.minOffset = minOffset;
        this.timestamp = timestamp;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(int lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}