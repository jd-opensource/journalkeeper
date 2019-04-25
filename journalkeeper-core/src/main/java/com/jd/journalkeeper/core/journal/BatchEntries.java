package com.jd.journalkeeper.core.journal;

/**
 * 批量Entry
 * @author liyue25
 * Date: 2019-04-25
 */
public class BatchEntries {
    /**
     * 偏移量，标识从这批entry的第几条开始读取。
     * 初始值为0
     */
    private final short offset;
    /**
     * 序列化的批Entry
     */
    private final byte [] entries;

    private final short size;
    public BatchEntries(byte[] entries, short offset, short size) {
        this.offset = offset;
        this.entries = entries;
        this.size = size;
    }

    public short getOffset() {
        return offset;
    }

    public byte[] getEntries() {
        return entries;
    }

    public short getSize() {
        return size;
    }
}
