package io.journalkeeper.core.entry.internal;

import io.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * Header(Legacy):
 *      Entry type      1 byte
 *
 * Header
 *      ENTRY_HEADER_MAGIC_CODE     1 byte // 为了兼容老版本header，使用固定值作为区分
 *      Entry type      2 bytes
 *      Entry version   2 bytes
 *
 *
 */
public abstract class InternalEntrySerializer<T extends InternalEntry> implements Serializer<T> {
    final static int HEADER_SIZE_ENTRY_TYPE_LEGACY = Byte.BYTES;
    final static byte ENTRY_HEADER_MAGIC_CODE = (byte) -233;
    final static int HEADER_SIZE_ENTRY_TYPE = Short.BYTES;
    final static int HEADER_SIZE_ENTRY_VERSION = Short.BYTES;
    @Override
    public final byte[] serialize(T entry) {

        final int headerSize = HEADER_SIZE_ENTRY_TYPE_LEGACY + (
                entry.getVersion() == InternalEntry.VERSION_LEGACY ?
                        0 : HEADER_SIZE_ENTRY_TYPE + HEADER_SIZE_ENTRY_VERSION);
        byte [] header = new byte[headerSize];
        ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        if (entry.getVersion() == InternalEntry.VERSION_LEGACY) {
            headerBuffer.put((byte) entry.getType().value());
        } else {
            headerBuffer.put(ENTRY_HEADER_MAGIC_CODE);
            headerBuffer.putShort((short) entry.getType().value());
            headerBuffer.putShort((short) entry.getVersion());
        }

        return serialize(entry, header);
    }

    @Override
    public final T parse(byte[] bytes) {
        int version, type;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (bytes[0] == ENTRY_HEADER_MAGIC_CODE) {
            buffer.get(); // magic code
            type = buffer.getShort();
            version = buffer.getShort();
        } else {
            version = InternalEntry.VERSION_LEGACY;
            type = buffer.get();
        }
        return parse(buffer, type, version);
    }

    protected abstract byte[] serialize(T entry, byte [] header);

    protected abstract T parse(ByteBuffer byteBuffer, int type, int version);
}
