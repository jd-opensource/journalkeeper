package com.jd.journalkeeper.persistence.local.journal;


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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * 带缓存的、无锁、高性能、多文件、基于位置的、Append Only的日志存储存储。
 * @author liyue25
 * Date: 2018/8/14
 */
public class PositioningStore implements JournalPersistence,Closeable {
    private final Logger logger = LoggerFactory.getLogger(PositioningStore.class);
    private File base;
    private final PreloadBufferPool bufferPool;
    private final NavigableMap<Long, StoreFile> storeFileMap = new ConcurrentSkipListMap<>();
    private long flushPosition = 0L;
    private long writePosition = 0L;
    private long leftPosition = 0L;
    // 正在写入的
    private StoreFile writeStoreFile = null;
    private Config config = null;
    public PositioningStore(PreloadBufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }


    /**
     * 将位置回滚到position
     * 与如下操作不能并发：
     * flush()
     * append()
     */
    public void truncate(long givenMax) throws IOException {
        if(givenMax == max()) return;
        logger.info("Rollback to position: {}, left: {}, right: {}, flushPosition: {}, store: {}...",
                 ThreadSafeFormat.formatWithComma(givenMax),
                ThreadSafeFormat.formatWithComma(leftPosition),
                ThreadSafeFormat.formatWithComma(writePosition),
                ThreadSafeFormat.formatWithComma(flushPosition),
                base.getAbsolutePath());

        if (givenMax <= leftPosition || givenMax > max()) {
            clear();
            this.leftPosition = givenMax;
            this.writePosition = givenMax;
            this.flushPosition = givenMax;
        } else if (givenMax < max()) {
            rollbackFiles(givenMax);
            this.writePosition = givenMax;
            if(this.flushPosition > givenMax) this.flushPosition = givenMax;
            resetWriteStoreFile();
        }
    }

    private void clear() {
        for(StoreFile storeFile :this.storeFileMap.values()) {
            if(storeFile.hasPage()) storeFile.unload();
            File file = storeFile.file();
            if(file.exists() && !file.delete())
                throw new TruncateException(String.format("Can not delete file: %s.", file.getAbsolutePath()));
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
            if(storeFile.position() + config.getFileDataSize() > writePosition) {
                writeStoreFile = storeFile;
            }
        }
    }

    public void recover(Path path, Properties properties) {
        this.base = path.toFile();
        this.config = toConfig(properties);

        recoverFileMap();

        long recoverPosition = this.storeFileMap.isEmpty()? 0L : this.storeFileMap.lastKey() + this.storeFileMap.lastEntry().getValue().fileDataSize();
        flushPosition = recoverPosition;
        writePosition = recoverPosition;
        leftPosition = this.storeFileMap.isEmpty()? 0L : this.storeFileMap.firstKey();

        resetWriteStoreFile();
        logger.info("Store loaded, left: {}, right: {},  base: {}.",
                ThreadSafeFormat.formatWithComma(min()),
                ThreadSafeFormat.formatWithComma(max()),
                base.getAbsolutePath());
    }

    private Config toConfig(Properties properties) {
        Config config = new Config();

        config.setCachedPageCount(Integer.parseInt(
                properties.getProperty(
                        Config.CACHED_PAGE_COUNT_KEY,
                        String.valueOf(Config.DEFAULT_CACHED_PAGE_COUNT))));
        config.setCacheLifeTime(Long.parseLong(
                properties.getProperty(
                        Config.CACHE_LIFETIME_MS_KEY,
                        String.valueOf(Config.DEFAULT_CACHE_LIFETIME_MS))));
        config.setFileDataSize(Integer.parseInt(
                properties.getProperty(
                        Config.FILE_DATA_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_FILE_DATA_SIZE))));
        config.setFileHeaderSize(Integer.parseInt(
                properties.getProperty(
                        Config.FILE_HEADER_SIZE_KEY,
                        String.valueOf(Config.DEFAULT_FILE_HEADER_SIZE))));

        return config;
    }

    private void recoverFileMap() {
        File[] files = base.listFiles(file -> file.isFile() && file.getName().matches("\\d+"));
        long filePosition;
        if(null != files) {
            for (File file : files) {
                filePosition = Long.parseLong(file.getName());
                storeFileMap.put(filePosition, new LocalStoreFile(filePosition, base, config.getFileHeaderSize(), bufferPool, config.getFileDataSize()));
            }
        }

        // 检查文件是否连续完整
        if(!storeFileMap.isEmpty()) {
            long position = storeFileMap.firstKey();
            for (Map.Entry<Long, StoreFile> fileEntry : storeFileMap.entrySet()) {
                if(position != fileEntry.getKey()) {
                    throw new CorruptedStoreException(String.format("Files are not continuous! expect: %d, actual file name: %d, store: %s.", position, fileEntry.getKey(), base.getAbsolutePath()));
                }
                position += fileEntry.getValue().file().length() - config.getFileHeaderSize();
            }
        }
    }



    public long append(ByteBuffer buffer) throws IOException{
        if (null == writeStoreFile) writeStoreFile = createStoreFile(writePosition);
        if (config.getFileDataSize() - writeStoreFile.writePosition() < buffer.remaining()) writeStoreFile = createStoreFile(writePosition);
        writePosition += writeStoreFile.append(buffer);
        return writePosition;
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
    public void flush() throws IOException {
        while (flushPosition < writePosition) {
            Map.Entry<Long, StoreFile> entry = storeFileMap.floorEntry(flushPosition);
            if (null == entry) return;
            StoreFile storeFile = entry.getValue();
            if (!storeFile.isClean()) storeFile.flush();
            if (flushPosition < storeFile.position() + storeFile.flushPosition()) {
                flushPosition = storeFile.position() + storeFile.flushPosition();
            }
        }
        evict();
    }


    private StoreFile createStoreFile(long position) {
        StoreFile storeFile = new LocalStoreFile(position, base, config.getFileHeaderSize(), bufferPool, config.getFileDataSize());
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
    private void evict() {
        if(storeFileMap.isEmpty()) return ;
        List<LruWrapper<StoreFile>> sorted;
        sorted = storeFileMap.values().stream()
                .filter(StoreFile::hasPage)
                .map(storeFile -> new LruWrapper<>(storeFile, storeFile.lastAccessTime()))
                .sorted(Comparator.comparing(LruWrapper::getLastAccessTime))
                .collect(Collectors.toList());

        long now = System.currentTimeMillis();
        int count = sorted.size();
        while (!sorted.isEmpty()) {
            LruWrapper<StoreFile> storeFileWrapper = sorted.remove(0);
            StoreFile storeFile = storeFileWrapper.get();
            if(storeFile.lastAccessTime() == storeFileWrapper.getLastAccessTime()
                    && (count > config.getCachedPageCount() // 已经超过缓存数量限制
                        || storeFileWrapper.getLastAccessTime() + config.getCacheLifeTime() > now)){ // 或者缓存太久没有被访问
                if(storeFile.unload()) {
                    count--;
                }
            }
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



    private void checkReadPosition(long position){
        long p;
        if((p = leftPosition) > position) {
            throw new PositionUnderflowException(position, p);
        } else if(position >= (p = writePosition)) {
            throw new PositionOverflowException(position, p);
        }

    }


    /**
     * 删除 position之前的文件
     */
    public long shrink(long givenMin) throws IOException {

        if(givenMin > flushPosition) givenMin = flushPosition;

        Iterator<Map.Entry<Long, StoreFile>> iterator =
                storeFileMap.entrySet().iterator();
        long deleteSize = 0L;

        while (iterator.hasNext()) {
            Map.Entry<Long, StoreFile> entry = iterator.next();
            StoreFile storeFile = entry.getValue();
            long start = entry.getKey();
            long fileDataSize = storeFile.hasPage()? storeFile.writePosition(): storeFile.fileDataSize();

            // 至少保留一个文件
            if(storeFileMap.size() < 2 || start + fileDataSize > givenMin) break;
            leftPosition += fileDataSize;
            iterator.remove();

            deleteSize += deleteStoreFile(storeFile);
        }

        return deleteSize;

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


    @Override
    public void close() {
        for(StoreFile storeFile : storeFileMap.values()) {
            storeFile.unload();
        }
    }

    public static class Config {
        final static int DEFAULT_FILE_HEADER_SIZE = 128;
        final static int DEFAULT_FILE_DATA_SIZE = 128 * 1024 * 1024;
        final static int DEFAULT_CACHED_PAGE_COUNT = 2;
        final static long DEFAULT_CACHE_LIFETIME_MS = 5000L;


        final static String FILE_HEADER_SIZE_KEY = "persistence.local.file_header_size";
        final static String FILE_DATA_SIZE_KEY = "persistence.local.file_data_size";
        final static String CACHED_PAGE_COUNT_KEY = "persistence.local.cached_page_count";
        final static String CACHE_LIFETIME_MS_KEY = "persistence.local.cache_lifetime_ms";
        /**
         * 文件头长度
         */
        private int fileHeaderSize;
        /**
         * 文件内数据最大长度
         */
        private int fileDataSize;
        /**
         * 最多缓存的页面数量
         */
        private int cachedPageCount;

        /**
         * 缓存最长存活时间
         */
        private long cacheLifeTime;

        int getFileHeaderSize() {
            return fileHeaderSize;
        }

        void setFileHeaderSize(int fileHeaderSize) {
            this.fileHeaderSize = fileHeaderSize;
        }

        int getFileDataSize() {
            return fileDataSize;
        }

        void setFileDataSize(int fileDataSize) {
            this.fileDataSize = fileDataSize;
        }

        int getCachedPageCount() {
            return cachedPageCount;
        }

        void setCachedPageCount(int cachedPageCount) {
            this.cachedPageCount = cachedPageCount;
        }

        long getCacheLifeTime() {
            return cacheLifeTime;
        }

        void setCacheLifeTime(long cacheLifeTime) {
            this.cacheLifeTime = cacheLifeTime;
        }
    }



}
