package com.jd.journalkeeper.core.api;

/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class RaftEntry {
    private byte [] entry;
    private  RaftEntryHeader header;

    public void setEntry(byte[] entry) {
        this.entry = entry;
    }

    public void setHeader(RaftEntryHeader header) {
        this.header = header;
    }

    public byte[] getEntry() {
        return entry;
    }

    public RaftEntryHeader getHeader() {
        return header;
    }
}
