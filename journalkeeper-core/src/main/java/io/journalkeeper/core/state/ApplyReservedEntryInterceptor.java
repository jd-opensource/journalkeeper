package io.journalkeeper.core.state;

import io.journalkeeper.core.api.JournalEntry;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public interface ApplyReservedEntryInterceptor {
    void applyReservedEntry(JournalEntry journalEntry, long index);
}
