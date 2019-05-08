package com.jd.journalkeeper.core.journal;


import com.jd.journalkeeper.core.api.RaftEntry;

import java.nio.ByteBuffer;

/**
 * 每个Entry Header 包括：
 *
 * Length: 4 bytes
 * Magic: 2 bytes
 * Term: 4 bytes
 * Partition: 2 bytes
 * Batch size: 2 bytes
 * Entry: Variable length
 *
 * @author liyue25
 * Date: 2019-03-19
 */
public class Entry implements RaftEntry {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XF4, (byte) 0X3C}).getShort();

    private byte [] entry;
    private final EntryHeader header;
    public Entry(){
        this.header = new EntryHeader();
    }

    public Entry(EntryHeader header, byte [] entry) {
        this.header = header;
        this.entry = entry;
    }

    public Entry(byte [] entry, int term, short partition){
        this();
        this.entry = entry;
        this.header.setTerm(term);
        this.header.setLength(EntryParser.getHeaderLength() + entry.length);
        this.header.setPartition(partition);
    }

    public byte [] getEntry() {
        return entry;
    }

    public int getTerm() {
        return header.getTerm();
    }

    public static long headerSize() {
        return 8;
    }

    public int getLength() {
        return header.getLength();
    }

    public void setLength(int length) {
        this.header.setLength(length);
    }

    public void setEntry(byte [] entry) {
        this.entry = entry;
    }

    public void setTerm(int term) {
        this.header.setTerm(term);
    }

    public short getPartition() {
        return this.header.getPartition();
    }

    public void setPartition(short partition) {
        this.header.setPartition(partition);
    }

    public short getBatchSize() {
        return header.getBatchSize();
    }

    public void setBatchSize(short batchSize) {
        this.header.setBatchSize(batchSize);
    }

    @Override
    public EntryHeader getHeader() {
        return header;
    }
}
