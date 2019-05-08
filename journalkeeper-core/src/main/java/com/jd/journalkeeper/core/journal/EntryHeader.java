package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.core.api.RaftEntryHeader;

import java.nio.ByteBuffer;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class EntryHeader implements RaftEntryHeader {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XF4, (byte) 0X3C}).getShort();
    private int term;
    private int length;
    private short partition = 0;
    private short batchSize = 1;
    @Override
    public short getPartition() {
        return partition;
    }

    @Override
    public int getLength() {
        return length;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setLength(int length) {
        this.length = length;
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
