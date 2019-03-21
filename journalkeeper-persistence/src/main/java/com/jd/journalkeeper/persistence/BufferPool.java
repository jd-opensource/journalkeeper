package com.jd.journalkeeper.persistence;

import java.nio.ByteBuffer;

public interface BufferPool {
    default ByteBuffer allocate(int capacity){
        return ByteBuffer.allocate(capacity);
    }

    default void release(ByteBuffer buffer) {}
}
