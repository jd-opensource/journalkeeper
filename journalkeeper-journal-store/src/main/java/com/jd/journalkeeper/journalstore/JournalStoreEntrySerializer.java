package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Header:
 * ----------
 * Entry count: 2 bytes
 *
 * Entries:
 * ----------
 * Entry length: 4 bytes
 * Entry body: variable
 * Entry length: 4 bytes
 * Entry body: variable
 * ...
 *
 *
 * @author liyue25
 * Date: 2019-04-24
 */
public class JournalStoreEntrySerializer implements Serializer<List<byte[]>> {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreEntrySerializer.class);
    @Override
    public int sizeOf(List<byte[]> entries) {
        return Integer.BYTES + entries.stream().mapToInt(entry -> entry.length).sum();
    }

    @Override
    public byte[] serialize(List<byte[]> entries) {
        byte [] bytes = new byte[sizeOf(entries)];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        if(entries.size() > Short.MAX_VALUE) {
            throw new TooManyEntriesException();
        }
        byteBuffer.putShort((short) entries.size());

        for (byte[] entry : entries) {
            byteBuffer.putInt(entry.length);
            byteBuffer.put(entry);
        }

        return bytes;
    }

    @Override
    public List<byte[]> parse(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int size = byteBuffer.getShort();
        if(size > 0) {
            List<byte[]> entries = new ArrayList<>(size);
            while (byteBuffer.hasRemaining()) {
                int length = byteBuffer.getInt();
                if(length < 0 || length > byteBuffer.remaining()) {
                    throw new ParseEntryException(String.format("Invalid entry length: %d at offset: %d.",
                            length, byteBuffer.position() - Integer.BYTES));
                }
                byte [] entryBytes = new byte[length];
                byteBuffer.get(entryBytes);
                entries.add(entryBytes);
            }
            return entries;
        } else {
            throw new ParseEntryException(String.format("Invalid entry count: %d.", size));
        }
    }
}
