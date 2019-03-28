package com.jd.journalkeeper.persistence.local.journal;


import com.jd.journalkeeper.utils.buffer.BufferHolder;
import com.jd.journalkeeper.utils.buffer.PreloadBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * 支持并发、带缓存页、顺序写入的文件
 */
public class LocalStoreFile implements StoreFile, BufferHolder {
    private static final Logger logger = LoggerFactory.getLogger(LocalStoreFile.class);
    // 文件全局位置
    private final long filePosition;
    // 文件头长度
    private final int headerSize;
    // 对应的File
    private final File file;
    // buffer读写锁：
    // 访问(包括读和写）buffer时加读锁；
    // 加载、释放buffer时加写锁；
    private  final StampedLock bufferLock = new StampedLock();
    // 缓存页
    private ByteBuffer pageBuffer = null;

    // 缓存页类型
    // 只读：
    // MAPPED_BUFFER：mmap映射内存镜像文件；
    // 读写：
    // DIRECT_BUFFER: 数据先写入DirectBuffer，异步刷盘到文件，性能最好；
    private final static int MAPPED_BUFFER = 0, DIRECT_BUFFER = 1, NO_BUFFER = -1;
    private int bufferType = NO_BUFFER;

    private PreloadBufferPool bufferPool;
    private int capacity;
    private long lastAccessTime = System.currentTimeMillis();

    // 当前刷盘位置
    private int flushPosition;
    // 当前写入位置
    private int writePosition = 0;

    private long timestamp = -1L;


    LocalStoreFile(long filePosition, File base, int headerSize, PreloadBufferPool bufferPool, int maxFileDataLength) {
        this.filePosition = filePosition;
        this.headerSize = headerSize;
        this.bufferPool = bufferPool;
        this.capacity = maxFileDataLength;
        this.file = new File(base, String.valueOf(filePosition));
        if(file.exists() && file.length() > headerSize) {
            this.writePosition = (int)(file.length() - headerSize);
            this.flushPosition = writePosition;
        }
    }


    @Override
    public File file() {
        return file;
    }

    @Override
    public long position() {
        return filePosition;
    }

    private void loadRoUnsafe() throws IOException{
            if (null != pageBuffer) throw new IOException("Buffer already loaded!");
            ByteBuffer loadBuffer;
            try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fileChannel = raf.getChannel()) {
                loadBuffer =
                        fileChannel.map(FileChannel.MapMode.READ_ONLY, headerSize, file.length() - headerSize);
            }
            pageBuffer = loadBuffer;
            bufferType = MAPPED_BUFFER;
            pageBuffer.clear();
    }

    private void loadRwUnsafe() throws IOException{
            if(bufferType == DIRECT_BUFFER ) {
                return;
            } else if(bufferType == MAPPED_BUFFER) {
                unloadUnsafe();
            }
            try {
                ByteBuffer buffer = bufferPool.allocate(capacity, this);
                loadDirectBuffer(buffer);
            } catch (OutOfMemoryError oom) {
                logger.warn("Insufficient direct memory, use write map instead. File: {}", file.getAbsolutePath());
            }
    }

    private void loadDirectBuffer(ByteBuffer buffer) throws IOException {
        if (file.exists() && file.length() > headerSize) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fileChannel = raf.getChannel()) {
                fileChannel.position(headerSize);
                int length;
                do {
                    length = fileChannel.read(buffer);
                } while (length > 0);
            }
            buffer.clear();
        }
        this.pageBuffer = buffer;
        bufferType = DIRECT_BUFFER;
    }

    public long timestamp() {
        if (timestamp == -1L) {
            // 文件存在初始化时间戳
            readTimestamp();
        }
        return timestamp;
    }

    private void readTimestamp() {
        ByteBuffer timeBuffer = ByteBuffer.allocate(8);
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(0);
            fileChannel.read(timeBuffer);
        } catch (Exception e) {
            logger.warn("Exception: ", e);
        } finally {
            timestamp = timeBuffer.getLong(0);
        }
    }

    private void writeTimestamp() {
        ByteBuffer timeBuffer = ByteBuffer.allocate(8);
        long creationTime = System.currentTimeMillis();
        timeBuffer.putLong(0, creationTime);
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
            fileChannel.position(0);
            fileChannel.write(timeBuffer);
            fileChannel.force(true);
        } catch (Exception e) {
            logger.warn("Exception:", e);
        } finally {
            timestamp = creationTime;
        }
    }

    @Override
    public boolean unload() {
        long stamp = bufferLock.writeLock();
        try {
            if(isClean()) {
                unloadUnsafe();
                return true;
            } else {
                return false;
            }
        }finally {
            bufferLock.unlockWrite(stamp);
        }
    }

    @Override
    public boolean hasPage() {
        return this.bufferType != NO_BUFFER;
    }


    @Override
    public ByteBuffer read(int position, int length) throws IOException{
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (!hasPage()) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if(ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRoUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if(rs != 0L) {
                stamp = rs;
            }
            ByteBuffer byteBuffer = pageBuffer.asReadOnlyBuffer();
            byteBuffer.position(position);
            byteBuffer.limit(writePosition);

            ByteBuffer dest = ByteBuffer.allocate(Math.min(length, byteBuffer.remaining()));
            if (length < byteBuffer.remaining()) {
                byteBuffer.limit(byteBuffer.position() + length);
            }
            dest.put(byteBuffer);
            dest.flip();
            return dest;

        } finally {
            bufferLock.unlock(stamp);
        }
    }

    // Not thread safe!
    private int appendToPageBuffer(ByteBuffer byteBuffer) {
        pageBuffer.position(writePosition);
        int writeLength = byteBuffer.remaining();
        pageBuffer.put(byteBuffer);

        writePosition += writeLength;
        return writeLength;
    }



    @Override
    public int append(ByteBuffer byteBuffer) throws IOException{
        touch();
        long stamp = bufferLock.readLock();
        try {
            while (bufferType != DIRECT_BUFFER ) {
                long ws = bufferLock.tryConvertToWriteLock(stamp);
                if(ws != 0L) {
                    // 升级成写锁成功
                    stamp = ws;
                    loadRwUnsafe();
                } else {
                    bufferLock.unlockRead(stamp);
                    stamp = bufferLock.writeLock();
                }
            }
            long rs = bufferLock.tryConvertToReadLock(stamp);
            if(rs != 0L) {
                stamp = rs;
            }
            return appendToPageBuffer(byteBuffer);
        } finally {
            bufferLock.unlock(stamp);
        }
    }

    private void touch() {
        lastAccessTime = System.currentTimeMillis();
    }

    private AtomicBoolean flushGate = new AtomicBoolean(false);

    /**
     * 刷盘
     */
    // Not thread safe!
    @Override
    public int flush() throws IOException {
        long stamp = bufferLock.readLock();
        try {
            if (writePosition > flushPosition) {
                if (flushGate.compareAndSet(false, true)) {
                    if (!file.exists()) {
                        // 第一次创建文件写入头部预留128字节中0位置开始的前8字节长度:文件创建时间戳
                        writeTimestamp();
                    }
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
                        return  flushPageBuffer(fileChannel);
                    } finally {
                        flushGate.compareAndSet(true, false);
                    }
                } else {
                    throw new ConcurrentModificationException();
                }
            }
            return 0;
        } finally {
            bufferLock.unlockRead(stamp);
        }
    }

    private int flushPageBuffer(FileChannel fileChannel) throws IOException {
        int flushEnd = writePosition;
        ByteBuffer flushBuffer = pageBuffer.asReadOnlyBuffer();
        flushBuffer.position(flushPosition);
        flushBuffer.limit(flushEnd);
        fileChannel.position(headerSize + flushPosition);
        int flushSize = flushEnd - flushPosition;

        while (flushBuffer.hasRemaining()) {
            fileChannel.write(flushBuffer);
        }
        flushPosition = flushEnd;
        return flushSize;
    }

    // Not thread safe!
    @Override
    public void rollback(int position) throws IOException {
        if(position < writePosition) {
            writePosition = position;
        }
        if (position < flushPosition) {
            if(flushGate.compareAndSet(false, true)) {
                try {
                    flushPosition = position;
                    try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fileChannel = raf.getChannel()) {
                        fileChannel.truncate(position + headerSize);
                    }
                }finally {
                    flushGate.compareAndSet(true, false);
                }
            } else {
                throw new ConcurrentModificationException();
            }
        }
    }

    @Override
    public boolean isClean() {
        return flushPosition >= writePosition;
    }

    @Override
    public int writePosition() {
        return writePosition;
    }

    @Override
    public int fileDataSize() {
        return Math.max((int)file.length() - headerSize, 0);
    }

    @Override
    public int flushPosition() {
        return flushPosition;
    }

    @Override
    public long lastAccessTime() {
        return lastAccessTime;
    }


    private void unloadUnsafe() {
        if (MAPPED_BUFFER == this.bufferType) {
            unloadMappedBuffer();
        } else if (DIRECT_BUFFER == this.bufferType) {
            unloadDirectBuffer();
        }
    }

    private void unloadDirectBuffer() {
        final ByteBuffer direct = pageBuffer;
        pageBuffer = null;
        this.bufferType = NO_BUFFER;
        if(null != direct) bufferPool.release(direct, this);
    }

    private void unloadMappedBuffer() {
        try {
            final Buffer mapped = pageBuffer;
            pageBuffer = null;
            this.bufferType = NO_BUFFER;
            if(null != mapped) {
                Method getCleanerMethod;
                getCleanerMethod = mapped.getClass().getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(mapped, new Object[0]);
                cleaner.clean();
            }
        }catch (Exception e) {
            logger.warn("Release direct buffer exception: ", e);
        }
    }
    @Override
    public int size() {
        return capacity;
    }

    @Override
    public boolean isFree() {
        return isClean();
    }

    @Override
    public boolean evict() {
        return unload();
    }

}
