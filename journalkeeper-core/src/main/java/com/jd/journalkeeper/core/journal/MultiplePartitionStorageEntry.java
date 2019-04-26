package com.jd.journalkeeper.core.journal;


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
public class MultiplePartitionStorageEntry {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XF4, (byte) 0X3C}).getShort();

    private byte [] entry;
    private int term;
    private int length;
    private short partition = 0;
    private short batchSize = 1;
    public MultiplePartitionStorageEntry(){}
    public MultiplePartitionStorageEntry(byte [] entry, int term){
        this.entry = entry;
        this.term = term;
        this.length = MultiplePartitionStorageEntryParser.getHeaderLength() + entry.length;
    }

    public MultiplePartitionStorageEntry(byte [] entry, int term, short partition){
        this.entry = entry;
        this.term = term;
        this.partition = partition;
        this.length = MultiplePartitionStorageEntryParser.getHeaderLength() + entry.length;
    }

    public byte [] getEntry() {
        return entry;
    }

    public int getTerm() {
        return term;
    }

    public static long headerSize() {
        return 8;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setEntry(byte [] entry) {
        this.entry = entry;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public short getPartition() {
        return partition;
    }

    public void setPartition(short partition) {
        this.partition = partition;
    }

    public short getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(short batchSize) {
        this.batchSize = batchSize;
    }
}
