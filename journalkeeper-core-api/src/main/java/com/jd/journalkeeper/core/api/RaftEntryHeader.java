package com.jd.journalkeeper.core.api;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class RaftEntryHeader {
    private int length;
    private short partition = 0;
    private short batchSize = 1;
    //Transient
    private short offset = 0;

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
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

    public short getOffset() {
        return offset;
    }

    public void setOffset(short offset) {
        this.offset = offset;
    }
}
