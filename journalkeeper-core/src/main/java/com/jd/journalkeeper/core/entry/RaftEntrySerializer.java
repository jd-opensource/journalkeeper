package com.jd.journalkeeper.core.entry;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.RaftEntry;
import com.jd.journalkeeper.core.api.RaftEntryHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Header
 * -----------
 * Length: 4 Bytes
 * Partition: 2 Bytes
 * BatchSize: 2 Bytes
 * Offset: 2 Bytes
 *
 * Data
 * -----------
 * Entry payload: Variable Bytes (HEADER_LENGTH - 10)
 *
 * @author liyue25
 * Date: 2019-04-24
 */
public class RaftEntrySerializer implements Serializer<RaftEntry> {
    public final static int HEADER_LENGTH = Integer.BYTES + Short.BYTES  + Short.BYTES + Short.BYTES;
    private static final Logger logger = LoggerFactory.getLogger(RaftEntrySerializer.class);
    @Override
    public int sizeOf(RaftEntry entries) {
        return entries.getHeader().getLength();
    }

    @Override
    public byte[] serialize(RaftEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(entry.getHeader().getLength());
        byteBuffer.putShort(entry.getHeader().getPartition());
        byteBuffer.putShort(entry.getHeader().getBatchSize());
        byteBuffer.putShort(entry.getHeader().getOffset());
        byteBuffer.put(entry.getEntry());
        return bytes;
    }

    @Override
    public RaftEntry parse(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        RaftEntry entry = new RaftEntry();
        RaftEntryHeader header = new RaftEntryHeader();
        entry.setHeader(header);
        header.setLength(byteBuffer.getInt());
        header.setPartition(byteBuffer.getShort());
        header.setBatchSize(byteBuffer.getShort());
        header.setOffset(byteBuffer.getShort());
        entry.setEntry(
                Arrays.copyOfRange(bytes,HEADER_LENGTH, bytes.length)
        );
        return entry;
    }
}
