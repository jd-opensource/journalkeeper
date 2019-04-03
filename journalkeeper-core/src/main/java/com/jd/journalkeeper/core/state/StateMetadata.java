package com.jd.journalkeeper.core.state;

import com.jd.journalkeeper.utils.files.DoubleCopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class StateMetadata extends DoubleCopy {
    private long lastApplied;
    private int lastIncludedTerm;
    public StateMetadata(File file) throws IOException {
        super(file, 128);
    }

    @Override
    protected String getName() {
        return "StateMetadata";
    }

    @Override
    protected byte[] serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        byteBuffer.putLong(getLastApplied());
        byteBuffer.putInt(getLastIncludedTerm());
        byteBuffer.flip();
        return byteBuffer.array();
    }

    @Override
    protected void parse(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        setLastApplied(byteBuffer.getLong());
        setLastIncludedTerm(byteBuffer.getInt());
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(int lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }
}
