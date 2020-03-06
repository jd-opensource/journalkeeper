package io.journalkeeper.persistence.local.cache;

import io.journalkeeper.utils.spi.Singleton;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * @author LiYue
 * Date: 2020/3/5
 */
public interface MemoryCacheManager extends Closeable {


    void printMetric();

    void addPreLoad(int bufferSize, int coreCount, int maxCount);

    void removePreLoad(int bufferSize);

    void allocateMMap(BufferHolder bufferHolder);

    ByteBuffer allocateDirect(int bufferSize, BufferHolder bufferHolder);

    void releaseDirect(ByteBuffer byteBuffer, BufferHolder bufferHolder);

    void releaseMMap(BufferHolder bufferHolder);

    Collection<PreloadCacheMetric> getCaches();

    long getMaxMemorySize();

    long getTotalUsedMemorySize();

    long getDirectUsedMemorySize();

    long getMapUsedMemorySize();
}
