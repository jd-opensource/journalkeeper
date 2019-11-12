package io.journalkeeper.core.transaction;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 *  transactionId       16 bytes
 *  timestamp           8 bytes
 *  type                1 byte
 *  partition           2 bytes
 *  commitOrAbort       1 byte
 *  batchSize           2 bytes
 *  entry               variable bytes
 *
 * @author LiYue
 * Date: 2019/10/22
 */
public class TransactionEntrySerializer implements Serializer<TransactionEntry> {
    private static final int FIXED_LENGTH = 30;

    @Override
    public byte[] serialize(TransactionEntry entry) {
        int size = FIXED_LENGTH + entry.getEntry().length;
        byte [] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // transactionId
        serializeUUID(entry.getTransactionId(), buffer);

        // timestamp
        buffer.putLong(entry.getTimestamp());

        // type
        buffer.put((byte )entry.getType().value());

        // partition
        buffer.putShort((short) entry.getPartition());
        // commitOrAbort
        buffer.put(entry.isCommitOrAbort() ? (byte ) 1 : (byte) 0);
        // batchSize
        buffer.putShort((short) entry.getBatchSize());
        // entry
        buffer.put(entry.getEntry());
        return bytes;
    }

    private void serializeUUID(UUID uuid, ByteBuffer buffer) {
        long mostSigBits = 0L;
        long leastSigBits = 0L;
        if(null != uuid) {
            mostSigBits = uuid.getMostSignificantBits();
            leastSigBits = uuid.getLeastSignificantBits();
        }
        buffer.putLong(mostSigBits);
        buffer.putLong(leastSigBits);
    }

    private UUID parseUUID(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();

        if (mostSigBits == 0L && leastSigBits == 0L) {
            return null;
        } else {
            return new UUID(mostSigBits, leastSigBits);
        }

    }

    @Override
    public TransactionEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte [] entryBytes = new byte[bytes.length - FIXED_LENGTH];
        buffer.position(FIXED_LENGTH);
        buffer.get(entryBytes);
        buffer.clear();
        return new TransactionEntry(
                parseUUID(buffer),
                buffer.getLong(),
                TransactionEntryType.valueOf(buffer.get()),
                buffer.getShort(),
                buffer.get() == (byte) 1,
                buffer.getShort(),
                entryBytes
        );
    }
}
