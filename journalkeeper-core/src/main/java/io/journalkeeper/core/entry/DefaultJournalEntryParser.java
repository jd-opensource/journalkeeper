package io.journalkeeper.core.entry;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;

import java.nio.ByteBuffer;

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
        return new DefaultJournalEntry(headerBytes, true, false);
    }

    @Override
    public JournalEntry parse(byte[] bytes) {
        return new DefaultJournalEntry(bytes, true, true);
    }

    @Override
    public JournalEntry createJournalEntry(byte[] payload) {
        int headerLength = headerLength();

        byte [] rawEntry = new byte[headerLength + payload.length];
        ByteBuffer buffer = ByteBuffer.wrap(rawEntry);
        JournalEntryParseSupport.setInt(buffer, JournalEntryParseSupport.LENGTH, rawEntry.length);
        JournalEntryParseSupport.setShort(buffer, JournalEntryParseSupport.MAGIC, DefaultJournalEntry.MAGIC_CODE);

        for (int i = 0; i < payload.length; i++) {
            rawEntry[headerLength + i] = payload[i];
        }
        return parse(rawEntry);

    }
}
