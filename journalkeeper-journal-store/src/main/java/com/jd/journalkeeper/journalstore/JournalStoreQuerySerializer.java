package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

public class JournalStoreQuerySerializer implements Serializer<JournalStoreQuery> {
    private static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES;
    @Override
    public int sizeOf(JournalStoreQuery partitionJournalStoreQuery) {
        return SIZE;
    }

    @Override
    public byte[] serialize(JournalStoreQuery query) {
        byte [] bytes = new byte[SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) query.getCmd());
        buffer.putShort((short) query.getPartition());
        buffer.putLong(query.getIndex());
        buffer.putInt(query.getSize());
        return bytes;
    }

    @Override
    public JournalStoreQuery parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new JournalStoreQuery(buffer.get(), buffer.getShort(), buffer.getLong(), buffer.getInt());
    }
}
