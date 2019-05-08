package com.jd.journalkeeper.journalstore;

import java.util.List;
import java.util.Map;

public class JournalStoreEntry {
    final static int CMD_APPEND = 0;
    final static int CMD_COMPACT = 1;
    final static int CMD_SCALE_PARTITIONS = 2;
    private final int cmd;
    private List<byte []> entries;
    private int [] partitions;
    private Map<Integer, Long> compactIndices;

    public JournalStoreEntry(List<byte []> entries) {
        this.cmd = CMD_APPEND;
        this.entries = entries;
    }

    public JournalStoreEntry(Map<Integer, Long> compactIndices) {
        this.cmd = CMD_COMPACT;
        this.compactIndices = compactIndices;
    }

    public JournalStoreEntry(int [] partitions) {
        this.cmd = CMD_SCALE_PARTITIONS;
        this.partitions = partitions;
    }


    public int getCmd() {
        return cmd;
    }

    public List<byte[]> getEntries() {
        return entries;
    }

    public int[] getPartitions() {
        return partitions;
    }

    public Map<Integer, Long> getCompactIndices() {
        return compactIndices;
    }
}
