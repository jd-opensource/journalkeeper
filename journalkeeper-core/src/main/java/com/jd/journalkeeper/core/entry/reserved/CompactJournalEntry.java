package com.jd.journalkeeper.core.entry.reserved;

import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class CompactJournalEntry extends ReservedEntry {
    private final Map<Integer, Long> compactIndices;
    public CompactJournalEntry(Map<Integer, Long> compactIndices) {
        super(TYPE_COMPACT_JOURNAL);
        this.compactIndices = compactIndices;
    }

    public Map<Integer, Long> getCompactIndices() {
        return compactIndices;
    }
}
