package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * 每个Entry Header 包括：
 *
 * Magic: 2 bytes
 * Term: 4 bytes
 * Length of entry: 4 bytes
 * Entry: Variable length
 *
 * @author liyue25
 * Date: 2019-03-19
 */
public class StorageEntry<E> {
    public final static short MAGIC = ByteBuffer.wrap(new byte[] {(byte) 0XC0, (byte) 0X7D}).getShort();
    private  E entry;
    private  int term;
    private int length;
    public StorageEntry(){}
    public StorageEntry(E entry, int term){
        this.entry = entry;
        this.term = term;
    }

    public E getEntry() {

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

    public void setEntry(E entry) {
        this.entry = entry;
    }

    public void setTerm(int term) {
        this.term = term;
    }

}
