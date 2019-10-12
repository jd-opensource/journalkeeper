package io.journalkeeper.core.entry;

import io.journalkeeper.core.api.BytesFragment;
import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.journal.ParseJournalException;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author LiYue
 * Date: 2019/10/12
 */
class DefaultJournalEntry implements JournalEntry {
    private final static short MAGIC_CODE = ByteBuffer.wrap(new byte[] {(byte) 0XF4, (byte) 0X3C}).getShort();
    private final static short MAGIC_CODE_NOT_SET = 0;

    // 包含Header和Payload
    protected final byte [] serializedBytes;
    private final ByteBuffer serializedBuffer;

    DefaultJournalEntry(byte [] serializedBytes) {
        this.serializedBytes = serializedBytes;
        this.serializedBuffer = ByteBuffer.wrap(serializedBytes);
        checkOrSetMagic();
    }

    private ByteBuffer serializedBuffer() {
        return serializedBuffer.slice();
    }
    private int offset = 0;
    protected void setLength(int length) {
        JournalEntryParseSupport.setInt(serializedBuffer(), JournalEntryParseSupport.LENGTH, length);
    }
    private void checkOrSetMagic() {
        short magic = JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.MAGIC);
        if(magic == MAGIC_CODE_NOT_SET) {
            JournalEntryParseSupport.setShort(serializedBuffer(), JournalEntryParseSupport.MAGIC, magicCode());
        } else if (magicCode() != magic) {
            throw new ParseJournalException("Check magic failed！");
        }
    }

    @Override
    public int getBatchSize() {
        return JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.BATCH_SIZE);
    }

    public void setBatchSize(int batchSize) {
        JournalEntryParseSupport.setShort(serializedBuffer(), JournalEntryParseSupport.BATCH_SIZE, (short) batchSize);
    }

    @Override
    public int getPartition() {
        return JournalEntryParseSupport.getShort(serializedBuffer(), JournalEntryParseSupport.PARTITION);
    }

    public void setPartition(int partition) {
        JournalEntryParseSupport.setShort(serializedBuffer(), JournalEntryParseSupport.PARTITION, (short) partition);
    }

    @Override
    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
    @Override
    public int getTerm() {
        return JournalEntryParseSupport.getInt(serializedBuffer(), JournalEntryParseSupport.TERM);
    }
    public void setTerm(int term) {
        JournalEntryParseSupport.setInt(serializedBuffer(), JournalEntryParseSupport.TERM, term);
    }

    @Override
    public BytesFragment getPayload() {
        return new BytesFragment(
                serializedBytes,
                JournalEntryParseSupport.getHeaderLength(),
                serializedBytes.length - JournalEntryParseSupport.getHeaderLength());
    }

    @Override
    public final byte[] getSerializedBytes() {
        return serializedBytes;
    }

    public static int minHeaderLength() {
        return JournalEntryParseSupport.getHeaderLength();
    }

    @Override
    public int getLength() {
        return JournalEntryParseSupport.getInt(serializedBuffer(), JournalEntryParseSupport.LENGTH);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultJournalEntry that = (DefaultJournalEntry) o;
        return Arrays.equals(serializedBytes, that.serializedBytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serializedBytes);
    }

    protected short magicCode() {return MAGIC_CODE;}
}
