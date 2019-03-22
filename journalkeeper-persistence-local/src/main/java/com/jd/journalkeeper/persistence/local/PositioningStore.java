package com.jd.journalkeeper.persistence.local;


import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.utils.ThreadSafeFormat;
import com.jd.journalkeeper.utils.buffer.PreloadBufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * 带缓存的、无锁、高性能、多文件、基于位置的、Append Only的日志存储存储。
 * @author liyue25
 * Date: 2018/8/14
 */
public class PositioningStore implements JournalPersistence,Closeable {
    private final Logger logger = LoggerFactory.getLogger(PositioningStore.class);
    private final int fileHeaderSize;
    private final int fileDataSize;
    private final int maxPageCount;
    private final long cacheLifeTime;
    private final int bufferLength;
    private final File base;
    private final PreloadBufferPool bufferPool;
    private final NavigableMap<Long, StoreFile> storeFileMap = new ConcurrentSkipListMap<>();
    private long flushPosition = 0L;
    private long writePosition = 0L;
    private long leftPosition = 0L;
    // 正在写入的
    private StoreFile writeStoreFile = null;

    public PositioningStore(File base, Config config, PreloadBufferPool bufferPool) {
        this.base = base;
        this.fileHeaderSize = config.fileHeaderSize;
        this.fileDataSize = config.fileDataSize;
        this.maxPageCount = config.cachedPageCount;
        this.cacheLifeTime = config.cacheLifeTime;
        this.bufferLength = config.bufferLength;
        this.bufferPool = bufferPool;
    }

    public long left() {
        return leftPosition;
    }

    public long right() {
        return writePosition;
    }
    public long flushPosition() {
        return flushPosition ;
    }

    /**
     * 将位置回滚到position
     * 与如下操作不能并发：
     * flush()
     * append()
     */
    public void setRight(long position) throws IOException {

        if(position == right()) return;
        logger.info("Rollback to position: {}, left: {}, right: {}, flushPosition: {}, store: {}...",
                 ThreadSafeFormat.formatWithComma(position),
                ThreadSafeFormat.formatWithComma(leftPosition),
                ThreadSafeFormat.formatWithComma(writePosition),
                ThreadSafeFormat.formatWithComma(flushPosition()),
                base.getAbsolutePath());

        if (position <= leftPosition || position > right()) {
            clear();
            this.leftPosition = position;
            this.writePosition = position;
            this.flushPosition = position;
        } else if (position < right()) {
            rollbackFiles(position);
            this.writePosition = position;
            if(this.flushPosition > position) this.flushPosition = position;
            resetWriteStoreFile();
        }
    }

    public void clear() {
        for(StoreFile storeFile :this.storeFileMap.values()) {
            if(storeFile.hasPage()) storeFile.unload();
            File file = storeFile.file();
            if(file.exists() && !file.delete())
                throw new RollBackException(String.format("Can not delete file: %s.", file.getAbsolutePath()));
        }
        this.storeFileMap.clear();
    }

    private void rollbackFiles(long position) throws IOException {

        if(!storeFileMap.isEmpty()) {
            // position 所在的Page需要截断至position
            Map.Entry<Long, StoreFile> entry = storeFileMap.floorEntry(position);
            StoreFile storeFile = entry.getValue();
            if(position > storeFile.position()) {
                int relPos = (int) (position - storeFile.position());
                logger.info("Truncate store file {} to relative position {}.", storeFile.file().getAbsolutePath(), relPos);
                storeFile.rollback(relPos);
            }

            SortedMap<Long, StoreFile> toBeRemoved = storeFileMap.tailMap(position);

            for(StoreFile sf : toBeRemoved.values()) {
                logger.info("Delete store file {}.", sf.file().getAbsolutePath());
                deleteStoreFile(sf);
            }
            toBeRemoved.clear();
        }


    }


    private void resetWriteStoreFile() {
        if(!storeFileMap.isEmpty()) {
            StoreFile storeFile = storeFileMap.lastEntry().getValue();
            if(storeFile.position() + fileDataSize > writePosition) {
                writeStoreFile = storeFile;
            }
        }
    }

    public void recover() throws IOException {
        recoverFileMap();

        long recoverPosition = this.storeFileMap.isEmpty()? 0L : this.storeFileMap.lastKey() + this.storeFileMap.lastEntry().getValue().fileDataSize();
        flushPosition = recoverPosition;
        writePosition = recoverPosition;
        leftPosition = this.storeFileMap.isEmpty()? 0L : this.storeFileMap.firstKey();

        resetWriteStoreFile();
        logger.info("Store loaded, left: {}, right: {},  base: {}.",
                ThreadSafeFormat.formatWithComma(left()),
                ThreadSafeFormat.formatWithComma(right()),
                base.getAbsolutePath());
    }

    private void recoverFileMap() {
        File[] files = base.listFiles(file -> file.isFile() && file.getName().matches("\\d+"));
        long filePosition;
        if(null != files) {
            for (File file : files) {
                filePosition = Long.parseLong(file.getName());
                storeFileMap.put(filePosition, new LocalStoreFile(filePosition, base, fileHeaderSize, bufferPool, fileDataSize));
            }
        }

        // 检查文件是否连续完整
        if(!storeFileMap.isEmpty()) {
            long position = storeFileMap.firstKey();
            for (Map.Entry<Long, StoreFile> fileEntry : storeFileMap.entrySet()) {
                if(position != fileEntry.getKey()) {
                    throw new CorruptedLogException(String.format("Files are not continuous! expect: %d, actual file name: %d, store: %s.", position, fileEntry.getKey(), base.getAbsolutePath()));
                }
                position += fileEntry.getValue().file().length() - fileHeaderSize;
            }
        }
    }



    public long append(ByteBuffer buffer) throws IOException{
        if (null == writeStoreFile) writeStoreFile = createStoreFile(writePosition);
        if (fileDataSize - writeStoreFile.writePosition() < buffer.remaining()) writeStoreFile = createStoreFile(writePosition);
        writePosition += writeStoreFile.append(buffer);
        return writePosition;
    }

    public long append(final List<ByteBuffer> buffers) throws IOException{
        if(null == buffers || buffers.isEmpty()) throw new WriteException("Parameter list is empty!");
        long position = 0;
        for (ByteBuffer buffer: buffers) {
            position = append(buffer);
        }
        return position;
    }

    @Override
    public long min() {
        return leftPosition;
    }

    @Override
    public long max() {
        return writePosition;
    }

    @Override
    public long flushed() {
        return flushPosition;
    }

    @Override
    public CompletableFuture<Long> flush() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (flushPosition < writePosition) {
                    Map.Entry<Long, StoreFile> entry = storeFileMap.floorEntry(flushPosition);
                    if (null == entry) return flushPosition;
                    StoreFile storeFile = entry.getValue();
                    if (!storeFile.isClean()) storeFile.flush();
                    if (flushPosition < storeFile.position() + storeFile.flushPosition()) {
                        flushPosition = storeFile.position() + storeFile.flushPosition();
                    } else {
                        // 永远不会走到这里，除非程序bug了。
                        throw new CorruptedLogException(String.format("ZERO length flushed! " +
                                        "File: position: %d, writePosition: %d, flushPosition: %d. " +
                                        "Store: writePosition: %d, flushPosition: %d, leftPosition: %d, store: %s.",
                                storeFile.position(), storeFile.writePosition(), storeFile.flushPosition(),
                                writePosition, flushPosition, leftPosition, base.getAbsolutePath()));
                    }
                }
                return flushPosition;
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        });
    }

    @Override
    public CompletableFuture<Void> truncate(long givenMax) {
        return null;
    }

    @Override
    public CompletableFuture<Long> shrink(long givenMin) {
        return null;
    }

    @Override
    public Long append(ByteBuffer... byteBuffers) {
        return null;
    }


    private StoreFile createStoreFile(long position) {
        StoreFile storeFile = new LocalStoreFile(position, base, fileHeaderSize, bufferPool, fileDataSize);
        StoreFile present;
        if((present = storeFileMap.putIfAbsent(position, storeFile)) != null){
            storeFile = present;
        }

        return storeFile;
    }

    private static class LruWrapper<V> {
        private final long lastAccessTime;
        private final V t;
        LruWrapper(V t, long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
            this.t = t;
        }
        private long getLastAccessTime() {
            return lastAccessTime;
        }

        private V get() {
            return t;
        }
    }

    /**
     * 清除文件缓存页。LRU。
     */
    public boolean evict() {
        boolean ret = false;
        if(storeFileMap.isEmpty()) return ret;
        List<LruWrapper<StoreFile>> sorted;
        sorted = storeFileMap.values().stream()
                .filter(StoreFile::hasPage)
                .map(storeFile -> new LruWrapper<StoreFile>(storeFile, storeFile.lastAccessTime()))
                .sorted(Comparator.comparing(LruWrapper::getLastAccessTime))
                .collect(Collectors.toList());

        long now = System.currentTimeMillis();
        int count = sorted.size();
        while (!sorted.isEmpty()) {
            LruWrapper<StoreFile> storeFileWrapper = sorted.remove(0);
            StoreFile storeFile = storeFileWrapper.get();
            if(storeFile.lastAccessTime() == storeFileWrapper.getLastAccessTime()
                    && (count > maxPageCount // 已经超过缓存数量限制
                        || storeFileWrapper.getLastAccessTime() + cacheLifeTime > now)){ // 或者缓存太久没有被访问
                if(storeFile.unload()) {
                    count--;
                    ret = true;
                }
            }
        }
        return ret;
    }
    public ByteBuffer read(long position) throws IOException{
        checkReadPosition(position);
        try {
            return tryRead(position);
        } catch (Throwable t) {
            logger.warn("Exception on read position {} of store {}.", position, base.getAbsolutePath(), t);
            throw t;
        }
    }

    public ByteBuffer read(long position, int length) throws IOException{
        checkReadPosition(position);
        try {
            StoreFile storeFile = storeFileMap.floorEntry(position).getValue();
            int relPosition = (int )(position - storeFile.position());
            return storeFile.read(relPosition, length);
        } catch (Throwable t) {
            logger.warn("Exception on read position {} of store {}.", position, base.getAbsolutePath(), t);
            throw t;
        }
    }

    @Override
    public void recover(Path path, Properties properties) {

    }

    private ByteBuffer tryRead(long position) throws IOException{

        checkReadPosition(position);
        StoreFile storeFile = storeFileMap.floorEntry(position).getValue();
        int relPosition = (int )(position - storeFile.position());
        return storeFile.read(relPosition, -1);
    }

    public List<ByteBuffer> batchRead(long position, int count) throws IOException{
        checkReadPosition(position);
        List<ByteBuffer> list = new ArrayList<>(count);
        long pointer = position;

        StoreFile storeFile = null;
        try {
            while (list.size() < count && pointer < writePosition) {

                if (null == storeFile || storeFile.writePosition() + storeFile.position() <= pointer) {
                    storeFile = storeFileMap.floorEntry(pointer).getValue();
                }

                int relPosition = (int) (pointer - storeFile.position());
                ByteBuffer buffer = storeFile.read(relPosition, -1);
                list.add(buffer);
                pointer += buffer.remaining();

            }

            return list;
        } catch (Throwable t) {
            logger.warn("Exception on batchRead position {} of store {}.", pointer, base.getAbsolutePath(), t);
            throw t;
        }

    }

    /**
     * 获取文件实际数据长度，不包含文件头
     */
    private long getDataSize(File file){
        long size = file.length();
        return size > fileHeaderSize ? size - fileHeaderSize: 0L;
    }


    private void checkReadPosition(long position){
        long p;
        if((p = leftPosition) > position) {
            throw new PositionUnderflowException(position, p);
        } else if(position >= (p = writePosition)) {
            throw new PositionOverflowException(position, p);
        }

    }

    public long physicalSize() {
        return storeFileMap.values().stream().map(StoreFile::file).mapToLong(File::length).sum();
    }

    /**
     * 删除 position之前的文件
     */
    public long physicalDeleteTo(long position) throws IOException {

        if(position > flushPosition) position = flushPosition;

        Iterator<Map.Entry<Long, StoreFile>> iterator =
                storeFileMap.entrySet().iterator();
        long deleteSize = 0L;

        while (iterator.hasNext()) {
            Map.Entry<Long, StoreFile> entry = iterator.next();
            StoreFile storeFile = entry.getValue();
            long start = entry.getKey();
            long fileDataSize = storeFile.hasPage()? storeFile.writePosition(): storeFile.fileDataSize();

            // 至少保留一个文件
            if(storeFileMap.size() < 2 || start + fileDataSize > position) break;
            leftPosition += fileDataSize;
            iterator.remove();

            deleteSize += deleteStoreFile(storeFile);
        }

        return deleteSize;

    }

    public boolean isClean() {
        return flushPosition == writePosition;
    }

    public long physicalDeleteLeftFile() throws IOException {
        if(storeFileMap.isEmpty()) return 0;
        StoreFile storeFile = storeFileMap.firstEntry().getValue();
        return physicalDeleteTo(storeFile.position() + (storeFile.hasPage() ? storeFile.writePosition(): storeFile.fileDataSize()));
    }

    private long deleteStoreFile(StoreFile storeFile) throws IOException {
        if(storeFile.isClean()) {
            storeFile.unload();
        }
        File file = storeFile.file();
        long fileSize = file.length();
        if(file.exists()) {
            if (file.delete()) {
                logger.debug("File {} deleted.", file.getAbsolutePath());
                return fileSize;
            } else {
                throw new IOException(String.format("Delete file %s failed!", file.getAbsolutePath()));
            }
        } else {
            return 0;
        }
    }


    public File base() {
        return base;
    }

    @Override
    public void close() {
        for(StoreFile storeFile : storeFileMap.values()) {
            storeFile.unload();
        }
    }


    public int fileCount() {
        return storeFileMap.size();
    }

    public boolean meetMinStoreFile(long minIndexedPhysicalPosition) {
        return storeFileMap.headMap(minIndexedPhysicalPosition).size() > 0;
    }

    public boolean isEarly(long timestamp, long minIndexedPhysicalPosition) {
        for (StoreFile storeFile : storeFileMap.headMap(minIndexedPhysicalPosition).values()) {
            if (storeFile.timestamp() < timestamp) {
                return true;
            }
        }
        return false;
    }

    public static class Config {
        public final static int DEFAULT_FILE_HEADER_SIZE = 128;
        public final static int DEFAULT_FILE_DATA_SIZE = 128 * 1024 * 1024;
        public final static int DEFAULT_CACHED_PAGE_COUNT = 2;
        public final static long DEFAULT_CACHE_LIFETIME_MS = 5000L;
        public final static int DEFAULT_BUFFER_LENGTH = 1024 * 1024;



        /**
         * 文件头长度
         */
        private final int fileHeaderSize;
        /**
         * 文件内数据最大长度
         */
        private final int fileDataSize;
        /**
         * 最多缓存的页面数量
         */
        private final int cachedPageCount;

        /**
         * 缓存最长存活时间
         */
        private final long cacheLifeTime;

        /**
         * 写文件的缓冲区长度
         */
        private final int bufferLength;


        public Config(){
            this(DEFAULT_FILE_DATA_SIZE, DEFAULT_CACHED_PAGE_COUNT,
                    DEFAULT_FILE_HEADER_SIZE,
                    DEFAULT_CACHE_LIFETIME_MS,
                    DEFAULT_BUFFER_LENGTH);
        }
        public Config(int fileDataSize){
            this(fileDataSize, DEFAULT_CACHED_PAGE_COUNT,
                    DEFAULT_FILE_HEADER_SIZE,
                    DEFAULT_CACHE_LIFETIME_MS,
                    DEFAULT_BUFFER_LENGTH);
        }

        public Config(int fileDataSize, int bufferLength){
            this(fileDataSize, DEFAULT_CACHED_PAGE_COUNT,
                    DEFAULT_FILE_HEADER_SIZE,
                    DEFAULT_CACHE_LIFETIME_MS,
                    bufferLength);
        }

        public Config(int fileDataSize, int cachedPageCount, int fileHeaderSize, long cacheLifeTime, int bufferLength){
            this.fileDataSize = fileDataSize;
            this.cachedPageCount = cachedPageCount;
            this.fileHeaderSize = fileHeaderSize;
            this.cacheLifeTime = cacheLifeTime;
            this.bufferLength = bufferLength;
        }
    }



}
