package io.journalkeeper.journalstore;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * @author LiYue
 * Date: 2019-08-12
 */
public class LongSerializer implements Serializer<Long> {
    @Override
    public byte[] serialize(Long entry) {
        byte [] bytes =  new byte[Long.BYTES];
        if(null == entry) {
            entry = -1L;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(entry);
        return bytes;
    }

    @Override
    public Long parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Long value = buffer.getLong();
        if(value < 0) value = null;
        return value;
    }
}
