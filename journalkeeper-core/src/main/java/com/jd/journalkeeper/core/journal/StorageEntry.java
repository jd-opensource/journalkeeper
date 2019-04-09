package com.jd.journalkeeper.core.journal;


import java.nio.ByteBuffer;

/**
 * 每个Entry Header 包括：
 *
 * Length: 4 bytes
 * Magic: 2 bytes
 * Term: 4 bytes
 * Type: 1 byte
 * Entry: Variable length
 *
 * @author liyue25
 * Date: 2019-03-19
 */
public class StorageEntry {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XC0, (byte) 0X7D}).getShort();

    public final static byte TYPE_LEADER_ANNOUNCEMENT = -0x01;
    public final static byte TYPE_STATE_DEFAULT = 0x01;
    private  byte [] entry;
    private  int term;
    private int length;
    private byte type = TYPE_STATE_DEFAULT;
    public StorageEntry(){}
    public StorageEntry(byte [] entry, int term){
        this.entry = entry;
        this.term = term;
        this.length = StorageEntryParser.getHeaderLength() + entry.length;
    }

    public StorageEntry(byte [] entry, int term, byte type){
        this.entry = entry;
        this.term = term;
        this.type = type;
        this.length = StorageEntryParser.getHeaderLength() + entry.length;
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

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }
}
