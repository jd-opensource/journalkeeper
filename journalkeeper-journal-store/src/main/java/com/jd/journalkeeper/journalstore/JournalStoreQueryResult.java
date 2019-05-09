package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.RaftEntry;

import java.util.List;
import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreQueryResult {
    private final int cmd;
    private final List<RaftEntry> entries;
    private final Map<Integer, Boundary> boundaries;

    private JournalStoreQueryResult(List<RaftEntry> entries, Map<Integer, Boundary> boundaries, int cmd) {
        this.entries = entries;
        this.boundaries = boundaries;
        this.cmd = cmd;
    }

    public JournalStoreQueryResult(List<RaftEntry> entries) {
        this(entries, null, JournalStoreQuery.CMQ_QUERY_ENTRIES);
    }


    public JournalStoreQueryResult( Map<Integer, Boundary> boundaries) {
        this(null, boundaries, JournalStoreQuery.CMQ_QUERY_PARTITIONS);
    }

    public static class Boundary {
        private final long min;
        private final long max;

        public Boundary(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }

    public int getCmd() {
        return cmd;
    }

    public List<RaftEntry> getEntries() {
        return entries;
    }

    public Map<Integer, Boundary> getBoundaries() {
        return boundaries;
    }
}
