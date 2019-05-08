package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * CMD_APPEND：
 * Command: 1 byte
 * Entry length: 4 bytes
 * Entry body: variable
 * Entry length: 4 bytes
 * Entry body: variable
 * ...
 *
 * CMD_COMPACT:
 * Command: 1 byte
 * Partition: 2 bytes
 * To index: 8 bytes
 * Partition: 2 bytes
 * To index: 8 bytes
 * Partition: 2 bytes
 * To index: 8 bytes
 * ...
 *
 * CMD_SCALE_PARTITIONS:
 * Command: 1 byte
 * Partition: 2 bytes
 * Partition: 2 bytes
 * Partition: 2 bytes
 * ...
 *
 * */
public class PartitionJournalStoreEntrySerializer implements Serializer<JournalStoreEntry> {
    @Override
    public int sizeOf(JournalStoreEntry journalStoreEntry) {
        switch (journalStoreEntry.getCmd()) {
            case JournalStoreEntry.CMD_APPEND:
                return sizeOfAppendEntry(journalStoreEntry);
            case JournalStoreEntry.CMD_COMPACT:
                return sizeOfCompact(journalStoreEntry);
            case JournalStoreEntry.CMD_SCALE_PARTITIONS:
                return sizeOfScalePartitions(journalStoreEntry);
            default:
                return 0;
        }
    }

    private int sizeOfScalePartitions(JournalStoreEntry journalStoreEntry) {
        return Byte.BYTES +  journalStoreEntry.getEntries().stream().mapToInt(entry -> entry.length).sum();
    }

    private int sizeOfCompact(JournalStoreEntry journalStoreEntry) {
        return Byte.BYTES +  (Short.BYTES + Long.BYTES) * journalStoreEntry.getCompactIndices().size();
    }

    private int sizeOfAppendEntry(JournalStoreEntry journalStoreEntry) {
        return Byte.BYTES + Short.BYTES * journalStoreEntry.getPartitions().length;
    }

    @Override
    public byte[] serialize(JournalStoreEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) entry.getCmd());
        switch (entry.getCmd()) {
            case JournalStoreEntry.CMD_APPEND:
                serializeAppendEntry(buffer, entry);
                break;
            case JournalStoreEntry.CMD_COMPACT:
                serializeCompact(buffer, entry);
                break;
            case JournalStoreEntry.CMD_SCALE_PARTITIONS:
                serializeScalePartitions(buffer, entry);
                break;
        }
        return bytes;
    }

    private void serializeScalePartitions(ByteBuffer buffer, JournalStoreEntry entry) {

    }

    private void serializeCompact(ByteBuffer buffer, JournalStoreEntry entry) {

    }

    private void serializeAppendEntry(ByteBuffer buffer, JournalStoreEntry entry) {

    }

    @Override
    public JournalStoreEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int cmd = buffer.get();
        switch (cmd) {
            case JournalStoreEntry.CMD_APPEND:
                return parseAppendEntry(buffer);
            case JournalStoreEntry.CMD_COMPACT:
                return parseCompact(buffer);
            case JournalStoreEntry.CMD_SCALE_PARTITIONS:
                return parseScalePartitions(buffer);
            default:
                return null;
        }
    }

    private JournalStoreEntry parseScalePartitions(ByteBuffer buffer) {
        return null;
    }

    private JournalStoreEntry parseCompact(ByteBuffer buffer) {
        return null;
    }

    private JournalStoreEntry parseAppendEntry(ByteBuffer buffer) {
        return null;
    }
}
