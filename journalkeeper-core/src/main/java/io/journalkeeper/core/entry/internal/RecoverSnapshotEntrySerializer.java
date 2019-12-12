package io.journalkeeper.core.entry.internal;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * RecoverSnapshotEntrySerializer
 *
 * type BYTE(1)
 * index LONG(8)
 *
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotEntrySerializer implements Serializer<RecoverSnapshotEntry> {

    @Override
    public byte[] serialize(RecoverSnapshotEntry entry) {
        int size = sizeOf(entry);
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        byteBuffer.put((byte) InternalEntryType.TYPE_RECOVER_SNAPSHOT.value());
        byteBuffer.putLong(entry.getIndex());
        return byteBuffer.array();
    }

    @Override
    public RecoverSnapshotEntry parse(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        RecoverSnapshotEntry result = new RecoverSnapshotEntry();
        buffer.get();
        result.setIndex(buffer.getLong());
        return result;
    }

    protected int sizeOf(RecoverSnapshotEntry entry) {
        return Byte.BYTES +  // type: 1 byte
                Long.BYTES; // index: 8 byte
    }
}