package com.jd.journalkeeper.core.entry.reserved;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-05-09
 *
 * Type: 1 byte
 * Partition: 2 bytes
 * Partition: 2 bytes
 * Partition: 2 bytes
 * ...
 *
 */
public class ScalePartitionsEntrySerializer implements Serializer<ScalePartitionsEntry> {
    @Override
    public int sizeOf(ScalePartitionsEntry scalePartitionsEntry) {
        return Byte.BYTES + Short.BYTES * scalePartitionsEntry.getPartitions().length;
    }

    @Override
    public byte[] serialize(ScalePartitionsEntry entry) {
        byte [] bytes = new byte[sizeOf(entry)];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.put((byte) entry.getType());
        for (int partition : entry.getPartitions()) {
            buffer.putShort((short) partition);
        }
        return bytes;
    }

    @Override
    public ScalePartitionsEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, Byte.BYTES, bytes.length - Byte.BYTES);
        List<Integer> partitionList = new LinkedList<>();
        while (buffer.hasRemaining()) {
            partitionList.add(Short.valueOf(buffer.getShort()).intValue());
        }
        return new ScalePartitionsEntry(partitionList.stream().mapToInt(Integer::intValue).toArray());
    }
}
