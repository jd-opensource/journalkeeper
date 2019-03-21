package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.Serializer;

import java.nio.ByteBuffer;

/**
 * 每个Entry Header 包括：
 *
 * Length 4 bytes
 * Term 4 bytes
 * entry 变长
 *
 * @author liyue25
 * Date: 2019-03-19
 */
public class StorageEntry<E> {
    private  E entry;
    private  int term;
    private int length;
    private final static int HEADER_SIZE = 8;
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

    public void writeHeader(ByteBuffer byteBuffer) {
        byteBuffer.putInt(length);
        byteBuffer.putInt(term);
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

    public static <E> StorageEntry<E> parse(ByteBuffer buffer, Serializer<E> serializer) {
        StorageEntry<E> storageEntry = new StorageEntry<>();
        storageEntry.setLength(buffer.getInt());
        storageEntry.setTerm(buffer.getInt());
        storageEntry.setEntry(serializer.parse(buffer));
        return storageEntry;
    }
}
