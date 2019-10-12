package io.journalkeeper.core.entry;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public class DefaultJournalEntryParser implements JournalEntryParser {
    @Override
    public int headerLength() {
        return JournalEntryParseSupport.getHeaderLength();
    }

    @Override
    public JournalEntry parseHeader(byte[] headerBytes) {
        DefaultJournalEntry entry = new DefaultJournalEntry(headerBytes);
        entry.setLength(entry.getLength());
        return entry;
    }

    @Override
    public JournalEntry parse(byte[] bytes) {
        DefaultJournalEntry entry = new DefaultJournalEntry(bytes);
        entry.setLength(bytes.length);
        return entry;
    }
}
