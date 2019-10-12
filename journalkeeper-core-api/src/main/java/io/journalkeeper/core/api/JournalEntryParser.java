package io.journalkeeper.core.api;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
public interface JournalEntryParser {
    int headerLength();
    JournalEntry parse(byte [] bytes);
    default JournalEntry parseHeader(byte [] headerBytes) {
        return parse(headerBytes);
    }

    default JournalEntry createJournalEntry(byte [] payload) {
        int headerLength = headerLength();
        byte [] rawEntry = new byte[headerLength + payload.length];
        for (int i = 0; i < payload.length; i++) {
            rawEntry[headerLength + i] = payload[i];
        }
        return parse(rawEntry);
    }
}
