package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

public class PartitionJournalStoreQuerySerializer implements Serializer<PartitionJournalStoreQuery> {
    private static final int SIZE = Byte.BYTES + Integer.BYTES + Long.BYTES;
    @Override
    public int sizeOf(PartitionJournalStoreQuery partitionJournalStoreQuery) {
        return SIZE;
    }

    @Override
    public byte[] serialize(PartitionJournalStoreQuery query) {
        byte [] bytes = new byte[SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) query.getCmd());
        buffer.putLong(query.getIndex());
        buffer.putInt(query.getSize());
        return bytes;
    }

    @Override
    public PartitionJournalStoreQuery parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new PartitionJournalStoreQuery(buffer.get(), buffer.getLong(), buffer.getInt());
    }
}
