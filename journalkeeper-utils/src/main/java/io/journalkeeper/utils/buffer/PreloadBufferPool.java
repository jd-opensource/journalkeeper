/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.utils.buffer;

import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import io.journalkeeper.utils.threads.Threads;
import io.journalkeeper.utils.threads.ThreadsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.VM;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author LiYue
 * Date: 2018-12-20
 */
public class PreloadBufferPool {
    private static final Logger logger = LoggerFactory.getLogger(PreloadBufferPool.class);
    private static final String PRELOAD_THREAD = "PreloadBuffer-PreloadThread";
    private static final String EVICT_THREAD = "PreloadBuffer-EvictThread";
    // 缓存比率：如果非堆内存使用率超过这个比率，就不再申请内存，抛出OOM。
    // 由于jvm在读写文件的时候会用到少量DirectBuffer作为缓存，必须预留一部分。
    private final static double CACHE_RATIO = 0.9d;
    /**
     * 缓存清理比率阈值，超过这个阈值执行缓存清理。
     */
    private static final double EVICT_RATIO = 0.9d;
    /**
     * 缓存核心利用率，系统会尽量将这个比率以内的内存用满。
     */
    private static final double CORE_RATIO = 0.8d;
    private final static long INTERVAL_MS = 50L;
    private final static String MAX_MEMORY_KEY = "PreloadBufferPool.MaxMemory";
    private static PreloadBufferPool instance = null;
    private final Threads threads = ThreadsFactory.create();
    // 可用的堆外内存上限，这个上限可以用JVM参数"PreloadBufferPool.MaxMemory"指定，
    // 默认为虚拟机最大内存（VM.maxDirectMemory()）的90%。
    private final long maxMemorySize;
    // 核心堆外内存大小，JournalKeeper总是尽量占满coreMemorySize内存用于缓存更多的文件，提升读写性能。
    private final long coreMemorySize;
    // 堆外内存超过evictMemorySize就会启动清理，清理的策略是LRU
    private final long evictMemorySize;
    private final AtomicLong usedSize = new AtomicLong(0L);
    private final Set<BufferHolder> directBufferHolders = ConcurrentHashMap.newKeySet();
    private final Set<BufferHolder> mMapBufferHolders = ConcurrentHashMap.newKeySet();
    private Map<Integer, PreLoadCache> bufferCache = new ConcurrentHashMap<>();

    private PreloadBufferPool() {

        maxMemorySize = Format.parseSize(System.getProperty(MAX_MEMORY_KEY), Math.round(VM.maxDirectMemory() * CACHE_RATIO));
        evictMemorySize = Math.round(maxMemorySize * EVICT_RATIO);
        coreMemorySize = Math.round(maxMemorySize * CORE_RATIO);

        threads.createThread(buildPreloadThread());
        threads.createThread(buildEvictThread());
        threads.start();

        logger.info("Max direct memory: {}, core direct memory: {}, evict direct memory: {}.",
                Format.formatSize(maxMemorySize),
                Format.formatSize(coreMemorySize),
                Format.formatSize(evictMemorySize));
    }

    public static PreloadBufferPool getInstance() {
        if (null == instance) {
            instance = new PreloadBufferPool();
        }
        return instance;
    }

    public static void close() {
        if (null != instance) {
            instance.threads.stop();
            instance.bufferCache.values().forEach(p -> {
                while (!p.cache.isEmpty()) {
                    instance.destroyOne(p.cache.remove());

                }
            });
            instance.directBufferHolders.parallelStream().forEach(BufferHolder::evict);
            instance.mMapBufferHolders.parallelStream().forEach(BufferHolder::evict);
            instance.bufferCache.values().forEach(p -> {
                while (!p.cache.isEmpty()) {
                    instance.destroyOne(p.cache.remove());

                }
            });
        }
        logger.info("Preload buffer pool closed.");
    }

    private AsyncLoopThread buildPreloadThread() {
        return ThreadBuilder.builder()
                .name(PRELOAD_THREAD)
                .sleepTime(INTERVAL_MS, INTERVAL_MS)
                .doWork(this::preLoadBuffer)
                .onException(e -> logger.warn("{} exception:", PRELOAD_THREAD, e))
                .daemon(true)
                .build();
    }

    private AsyncLoopThread buildEvictThread() {
        return ThreadBuilder.builder()
                .name(EVICT_THREAD)
                .sleepTime(INTERVAL_MS, INTERVAL_MS)
                .condition(() -> usedSize.get() > maxMemorySize * EVICT_RATIO)
                .doWork(this::evict)
                .onException(e -> logger.warn("{} exception:", EVICT_THREAD, e))
                .daemon(true)
                .build();
    }

    /**
     * 清除文件缓存页。LRU。
     */
    private synchronized void evict() {

        // 清理超过maxCount的缓存页
        for (PreLoadCache preLoadCache : bufferCache.values()) {
            if (!needEviction()) {
                break;
            }
            while (preLoadCache.cache.size() > preLoadCache.maxCount && !needEviction()) {
                try {
                    destroyOne(preLoadCache.cache.remove());
                } catch (NoSuchElementException ignored) {
                }
            }
        }

        // 清理使用中最旧的页面，直到内存占用率达标
        if (needEviction()) {
            List<LruWrapper<BufferHolder>> sorted;
            sorted = Stream.concat(directBufferHolders.stream(), mMapBufferHolders.stream())
                    .filter(BufferHolder::isFree)
                    .map(bufferHolder -> new LruWrapper<>(bufferHolder, bufferHolder.lastAccessTime()))
                    .sorted(Comparator.comparing(LruWrapper::getLastAccessTime))
                    .collect(Collectors.toList());

            while (needEviction() && !sorted.isEmpty()) {
                LruWrapper<BufferHolder> wrapper = sorted.remove(0);
                BufferHolder holder = wrapper.get();
                if (holder.lastAccessTime() == wrapper.getLastAccessTime()) {
                    holder.evict();
                }
            }
        }
    }

    private boolean needEviction() {
        return usedSize.get() > evictMemorySize;
    }

    private boolean isOutOfMemory() {
        return usedSize.get() > maxMemorySize;
    }

    private boolean isHungry() {
        return usedSize.get() < coreMemorySize;
    }

    public synchronized void addPreLoad(int bufferSize, int coreCount, int maxCount) {
        PreLoadCache preLoadCache = bufferCache.putIfAbsent(bufferSize, new PreLoadCache(bufferSize, coreCount, maxCount));
        if (null != preLoadCache) {
            preLoadCache.referenceCount.incrementAndGet();
        }
    }

    public synchronized void removePreLoad(int bufferSize) {
        PreLoadCache preLoadCache = bufferCache.get(bufferSize);
        if (null != preLoadCache) {
            if (preLoadCache.referenceCount.decrementAndGet() <= 0) {
                bufferCache.remove(bufferSize);
                preLoadCache.cache.forEach(this::destroyOne);
            }
        }
    }

    private void destroyOne(ByteBuffer byteBuffer) {
        usedSize.getAndAdd(-1 * byteBuffer.capacity());
        releaseIfDirect(byteBuffer);
    }

    private void preLoadBuffer() {
        for (PreLoadCache preLoadCache : bufferCache.values()) {
            if (preLoadCache.cache.size() < preLoadCache.coreCount) {
                if (isHungry()) {
                    try {
                        while (preLoadCache.cache.size() < preLoadCache.coreCount && usedSize.get() + preLoadCache.bufferSize < maxMemorySize) {
                            preLoadCache.cache.add(createOne(preLoadCache.bufferSize));
                        }
                    } catch (OutOfMemoryError ignored) {
                        return;
                    }
                } else {
                    List<LruWrapper<BufferHolder>> outdated = directBufferHolders.stream()
                            .filter(b -> b.size() == preLoadCache.bufferSize)
                            .filter(BufferHolder::isFree)
                            .map(bufferHolder -> new LruWrapper<>(bufferHolder, bufferHolder.lastAccessTime()))
                            .sorted(Comparator.comparing(LruWrapper::getLastAccessTime)).collect(Collectors.toList());
                    while (preLoadCache.cache.size() < preLoadCache.coreCount && !outdated.isEmpty()) {
                        LruWrapper<BufferHolder> wrapper = outdated.remove(0);
                        BufferHolder holder = wrapper.get();
                        if (holder.lastAccessTime() == wrapper.getLastAccessTime()) {
                            holder.evict();
                        }
                    }
                }
            }
        }
    }

    private ByteBuffer createOne(int size) {
        reserveMemory(size);
        return ByteBuffer.allocateDirect(size);
    }

    private void reserveMemory(int size) {
        usedSize.addAndGet(size);
        try {
            while (isOutOfMemory()) {
                PreLoadCache preLoadCache = bufferCache.values().stream()
                        .filter(p -> p.cache.size() > 0)
                        .findAny().orElse(null);
                if (null != preLoadCache) {
                    destroyOne(preLoadCache.cache.remove());
                } else {
                    break;
                }
            }

            if (isOutOfMemory()) {
                // 如果内存不足，唤醒清理线程立即执行清理
                threads.wakeupThread(EVICT_THREAD);
                // 等待5x10ms，如果还不足抛出异常
                for (int i = 0; i < 5 && isOutOfMemory(); i++) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted: ", e);
                    }
                }
                if (isOutOfMemory()) {
                    throw new OutOfMemoryError();
                }
            }
        } catch (Throwable t) {
            usedSize.getAndAdd(-1 * size);
        }
    }

    private void releaseIfDirect(ByteBuffer byteBuffer) {
        if (byteBuffer instanceof DirectBuffer) {
            try {
                Method getCleanerMethod;
                getCleanerMethod = byteBuffer.getClass().getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(byteBuffer, new Object[0]);
                cleaner.clean();
            } catch (Exception e) {
                logger.warn("Exception: ", e);
            }
        }
    }

    public void allocateMMap(BufferHolder bufferHolder) {
        reserveMemory(bufferHolder.size());
        mMapBufferHolders.add(bufferHolder);
    }

    public ByteBuffer allocateDirect(int bufferSize, BufferHolder bufferHolder) {
        ByteBuffer buffer = allocateDirect(bufferSize);
        directBufferHolders.add(bufferHolder);
        return buffer;
    }

    private ByteBuffer allocateDirect(int bufferSize) {
        try {
            PreLoadCache preLoadCache = bufferCache.get(bufferSize);
            if (null != preLoadCache) {
                try {
                    ByteBuffer byteBuffer = preLoadCache.cache.remove();
                    preLoadCache.onFlyCounter.getAndIncrement();
                    return byteBuffer;
                } catch (NoSuchElementException e) {
                    logger.warn("Pool is empty, create ByteBuffer: {}", Format.formatSize(bufferSize));
                    ByteBuffer byteBuffer = createOne(bufferSize);
                    preLoadCache.onFlyCounter.getAndIncrement();
                    threads.wakeupThread(PRELOAD_THREAD);
                    return byteBuffer;
                }
            } else {
                logger.warn("No cached buffer in pool, create ByteBuffer: {}", Format.formatSize(bufferSize));
                return createOne(bufferSize);

            }
        } catch (OutOfMemoryError outOfMemoryError) {
            logger.debug("OOM: {}/{}.", Format.formatSize(usedSize.get()), Format.formatSize(maxMemorySize));
            throw outOfMemoryError;
        }
    }

    public void releaseDirect(ByteBuffer byteBuffer, BufferHolder bufferHolder) {
        directBufferHolders.remove(bufferHolder);
        int size = byteBuffer.capacity();
        PreLoadCache preLoadCache = bufferCache.get(size);
        if (null != preLoadCache) {
            if (needEviction() && preLoadCache.cache.size() >= preLoadCache.maxCount) {
                destroyOne(byteBuffer);
            } else {
                byteBuffer.clear();
                preLoadCache.cache.add(byteBuffer);
            }
            preLoadCache.onFlyCounter.getAndDecrement();
        } else {
            destroyOne(byteBuffer);
        }
    }


    public void releaseMMap(BufferHolder bufferHolder) {
        mMapBufferHolders.remove(bufferHolder);
        usedSize.getAndAdd(-1 * bufferHolder.size());

    }

    public Collection<PreLoadCache> getCaches() {
        return new ArrayList<>(bufferCache.values());
    }

    public long getMaxMemorySize() {
        return maxMemorySize;
    }

    public long getTotalUsedMemorySize() {
        return usedSize.get();
    }

    public long getDirectUsedMemorySize() {
        return directBufferHolders.stream().mapToLong(BufferHolder::size).sum() +
                bufferCache.values().stream().mapToLong(c -> c.getBufferSize() * c.getCachedCount()).sum();
    }

    public long getMapUsedMemorySize() {
        return mMapBufferHolders.stream().mapToLong(BufferHolder::size).sum();
    }

    public static class PreLoadCache {
        private final int bufferSize;
        private final int coreCount, maxCount;
        private final Queue<ByteBuffer> cache = new ConcurrentLinkedQueue<>();
        private final AtomicInteger onFlyCounter = new AtomicInteger(0);
        private final AtomicInteger referenceCount;

        PreLoadCache(int bufferSize, int coreCount, int maxCount) {
            this.bufferSize = bufferSize;
            this.coreCount = coreCount;
            this.maxCount = maxCount;
            this.referenceCount = new AtomicInteger(1);
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public int getCoreCount() {
            return coreCount;
        }

        public int getMaxCount() {
            return maxCount;
        }

        public int getUsedCount() {
            return onFlyCounter.get();
        }

        public int getCachedCount() {
            return cache.size();
        }
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
}
