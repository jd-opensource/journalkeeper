package com.jd.journalkeeper.core.entry;


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
public class Entry extends RaftEntry {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XF4, (byte) 0X3C}).getShort();

    public Entry(){
        setHeader(new EntryHeader());
    }

    public Entry(EntryHeader header, byte [] entry) {
        setHeader(header);
        setEntry(entry);
    }

    public Entry(byte [] entry, int term, short partition){
        this();
        setEntry(entry);
        ((EntryHeader) getHeader()).setTerm(term);
        getHeader().setLength(EntryParser.getHeaderLength() + entry.length);
        getHeader().setPartition(partition);
    }


}
