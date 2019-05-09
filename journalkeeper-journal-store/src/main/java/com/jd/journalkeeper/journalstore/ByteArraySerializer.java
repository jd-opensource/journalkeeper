package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class ByteArraySerializer implements Serializer<byte[]> {
    @Override
    public int sizeOf(byte[] bytes) {
        return bytes.length;
    }

    @Override
    public byte[] serialize(byte[] entry) {
        return entry;
    }

    @Override
    public byte[] parse(byte[] bytes) {
        return bytes;
    }
}
