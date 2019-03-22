package com.jd.journalkeeper.persistence.local;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface StoreFile extends Timed {
    File file();
    long position();
    boolean unload();
    boolean hasPage();
    ByteBuffer read(int position, int length) throws IOException;
    int append(ByteBuffer buffer) throws IOException;
    int flush() throws IOException;
    void rollback(int position) throws IOException;
    boolean isClean();
    int writePosition();
    int fileDataSize();
    int flushPosition();
    long timestamp();
}
