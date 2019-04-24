package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalStoreQuery implements Serializer<JournalStoreQuery> {
    private final long index;
    private final int size;

    public JournalStoreQuery(){this(-1L, -1);}
    public JournalStoreQuery(long index, int size) {
        this.index = index;
        this.size = size;
    }

    public long getIndex() {
        return index;
    }

    public int getSize() {
        return size;
    }

    @Override
    public int sizeOf(JournalStoreQuery journalStoreQuery) {
        return Long.BYTES + Integer.BYTES;
    }

    @Override
    public byte[] serialize(JournalStoreQuery query) {
        byte [] bytes = new byte[Long.BYTES + Integer.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putLong(query.index);
        byteBuffer.putInt(query.size);
        return bytes;
    }

    @Override
    public JournalStoreQuery parse(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        return new JournalStoreQuery(byteBuffer.getLong(), byteBuffer.getInt());
    }

    public byte [] serialize() {
        return serialize(this);
    }
}
