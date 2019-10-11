package io.journalkeeper.core.entry;

import io.journalkeeper.base.FixedLengthSerializer;

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019/10/11
 */
public class DefaultEntryHeaderSerializer implements FixedLengthSerializer<EntryHeader> {

    @Override
    public byte[] serialize(EntryHeader header) {
        byte [] serializedHeader = new byte [serializedEntryLength()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(serializedHeader);
        JournalEntryParser.serializeHeader(byteBuffer, header);
        return serializedHeader;
    }

    @Override
    public EntryHeader parse(byte[] bytes) {
        return JournalEntryParser.parseHeader(ByteBuffer.wrap(bytes));
    }

    @Override
    public int serializedEntryLength() {
        return JournalEntryParser.getHeaderLength();
    }
}
