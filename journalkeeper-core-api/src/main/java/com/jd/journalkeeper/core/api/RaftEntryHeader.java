package com.jd.journalkeeper.core.api;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class RaftEntryHeader {
    private int payloadLength;
    private int partition = 0;
    private int batchSize = 1;
    //Transient
    private int offset = 0;

    public int getPayloadLength() {
        return payloadLength;
    }

    public void setPayloadLength(int payloadLength) {
        this.payloadLength = payloadLength;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
