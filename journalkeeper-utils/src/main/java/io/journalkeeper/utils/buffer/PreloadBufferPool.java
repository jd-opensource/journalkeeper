/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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

/**
 * @author LiYue
 * Date: 2018-12-20
 */
public class PreloadBufferPool {
    private static final Logger logger = LoggerFactory.getLogger(PreloadBufferPool.class);
    private Map<Integer,PreLoadCache> bufferCache = new ConcurrentHashMap<>();
    private final Threads threads = ThreadsFactory.create();
    private static final String PRELOAD_THREAD = "PreloadBuffer-PreloadThread";
    private static final String METRIC_THREAD = "PreloadBuffer-MetricThread";
    private static final String EVICT_THREAD = "PreloadBuffer-EvictThread";
    private final long cacheLifetimeMs;
    private final long maxMemorySize;
    // 缓存比率：如果非堆内存使用率超过这个比率，就不再申请内存，抛出OOM。
    // 由于jvm在读写文件的时候会用到少量DirectBuffer作为缓存，必须预留一部分。
    private final static double CACHE_RATIO = 0.9d;
    /**
     * 缓存清理比率阈值，超过这个阈值执行缓存清理。
     */
    private final static double EVICT_RATIO = 0.8d;
    private final static long DEFAULT_CACHE_LIFE_TIME_MS = 60000L;
    private final static long INTERVAL_MS = 50L;

    private final static String CACHE_LIFE_TIME_MS_KEY = "PreloadBufferPool.CacheLifeTimeMs";
    private final static String PRINT_METRIC_INTERVAL_MS_KEY = "PreloadBufferPool.PrintMetricIntervalMs";
    private final static String MAX_MEMORY_KEY = "PreloadBufferPool.MaxMemory";

    private final AtomicLong usedSize = new AtomicLong(0L);
    private final Set<BufferHolder> directBufferHolders = ConcurrentHashMap.newKeySet();
    private final Set<BufferHolder> mMapBufferHolders = ConcurrentHashMap.newKeySet();

    private static PreloadBufferPool instance = null;

    public static PreloadBufferPool getInstance() {
        if(null == instance) {
            instance = new PreloadBufferPool();
        }
        return instance;
    }

    private PreloadBufferPool() {
        long printMetricInterval = Long.parseLong(System.getProperty(PRINT_METRIC_INTERVAL_MS_KEY,"0"));
        this.cacheLifetimeMs = Long.parseLong(System.getProperty(CACHE_LIFE_TIME_MS_KEY,String.valueOf(DEFAULT_CACHE_LIFE_TIME_MS)));
        long maxMemorySize = Format.parseSize(System.getProperty(MAX_MEMORY_KEY), Math.round(VM.maxDirectMemory() * CACHE_RATIO));

        threads.createThread(buildPreloadThread());

        if(printMetricInterval > 0) {
            threads.createThread(buildMetricThread(printMetricInterval));
        }
        threads.createThread(buildEvictThread());
        threads.start();
        this.maxMemorySize = maxMemorySize;
        logger.info("Max direct memory size : {}.", Format.formatSize(maxMemorySize));
    }

    private AsyncLoopThread buildMetricThread(long printMetricInterval) {
        return ThreadBuilder.builder()
                .name(METRIC_THREAD)
                .sleepTime(printMetricInterval, printMetricInterval)
                .doWork(() -> {
                    long used = usedSize.get();
                    long plUsed = bufferCache.values().stream().mapToLong(preLoadCache -> {
                        long cached = preLoadCache.cache.size();
                        long usedPreLoad = preLoadCache.onFlyCounter.get();
                        long totalSize = preLoadCache.bufferSize * (cached + usedPreLoad);
                        logger.info("PreloadCache usage: cached: {} * {} = {}, used: {} * {} = {}, total: {}",
                                Format.formatSize(preLoadCache.bufferSize), cached, Format.formatSize(preLoadCache.bufferSize * cached),
                                Format.formatSize(preLoadCache.bufferSize), usedPreLoad, Format.formatSize(preLoadCache.bufferSize * usedPreLoad),
                                Format.formatSize(totalSize));
                        return totalSize;
                    }).sum();
                    logger.info("DirectBuffer preload/used/max: {}/{}/{}.",
                            Format.formatSize(plUsed),
                            Format.formatSize(used),
                            Format.formatSize(maxMemorySize));

                })
                .daemon(true)
                .build();
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
        // 先清除过期的
        for(BufferHolder holder: directBufferHolders) {
            if(System.currentTimeMillis() - holder.lastAccessTime() > cacheLifetimeMs){
                holder.evict();
            }
        }

        mMapBufferHolders.removeIf(holder -> System.currentTimeMillis() - holder.lastAccessTime() > cacheLifetimeMs && holder.evict());


        // 清理超过maxCount的缓存页
        for(PreLoadCache preLoadCache: bufferCache.values()) {
            while (preLoadCache.cache.size() > preLoadCache.maxCount) {
                if(usedSize.get() < maxMemorySize * EVICT_RATIO) {
                    return;
                }
                try {
                    destroyOne(preLoadCache.cache.remove());
                } catch (NoSuchElementException ignored) {}
            }
        }

        // 清理使用中最旧的页面，直到内存占用率达标

        if(usedSize.get() > maxMemorySize * EVICT_RATIO) {
            List<LruWrapper<BufferHolder>> sorted;
            sorted = directBufferHolders.stream()
                    .filter(BufferHolder::isFree)
                    .map(bufferHolder -> new LruWrapper<>(bufferHolder, bufferHolder.lastAccessTime()))
                    .sorted(Comparator.comparing(LruWrapper::getLastAccessTime))
                    .collect(Collectors.toList());

            while (usedSize.get() > maxMemorySize * EVICT_RATIO && !sorted.isEmpty() ) {
                LruWrapper<BufferHolder> wrapper = sorted.remove(0);
                BufferHolder holder = wrapper.get();
                if (holder.lastAccessTime() == wrapper.getLastAccessTime()) {
                    holder.evict();
                }
            }
        }


    }

    public synchronized void addPreLoad(int bufferSize, int coreCount, int maxCount) {
        PreLoadCache preLoadCache =  bufferCache.putIfAbsent(bufferSize, new PreLoadCache(bufferSize, coreCount, maxCount));
        if(null != preLoadCache) {
            preLoadCache.referenceCount.incrementAndGet();
        }
    }

    public synchronized void removePreLoad(int bufferSize) {
        PreLoadCache preLoadCache =  bufferCache.get(bufferSize);
        if(null != preLoadCache) {
            if(preLoadCache.referenceCount.decrementAndGet() <= 0) {
                bufferCache.remove(bufferSize);
                preLoadCache.cache.forEach(this::destroyOne);
            }
        }
    }

    public static void close() {
        if(null != instance) {
            instance.threads.stop();
            instance.bufferCache.values().forEach(p -> {
                while (!p.cache.isEmpty()) {
                    instance.destroyOne(p.cache.remove());

                }
            });
            instance.directBufferHolders.parallelStream().forEach(BufferHolder::evict);
            instance.mMapBufferHolders.parallelStream().forEach(BufferHolder::evict);
        }
        logger.info("Preload buffer pool closed.");
    }

    private void destroyOne(ByteBuffer byteBuffer) {
        usedSize.getAndAdd(-1 * byteBuffer.capacity());
//        logger.info("Release direct {}/{}",
//                Format.formatSize(byteBuffer.capacity()),
//                Format.formatSize(usedSize.get()));
        releaseIfDirect(byteBuffer);
    }

    private synchronized void preLoadBuffer() {

        for(PreLoadCache preLoadCache: bufferCache.values()) {
            try {
                while (preLoadCache.cache.size() < preLoadCache.coreCount && usedSize.get() + preLoadCache.bufferSize < maxMemorySize) {
                    preLoadCache.cache.add(createOne(preLoadCache.bufferSize));
                }
            } catch (OutOfMemoryError ignored) {}
        }
    }

    private ByteBuffer createOne(int size) {
        reserveMemory(size);
//        logger.info("Allocate direct {}/{}",
//                Format.formatSize(size),
//                Format.formatSize(usedSize.get()));

        return ByteBuffer.allocateDirect(size);
    }

    private void reserveMemory(int size) {
        while (usedSize.get() + size > maxMemorySize) {
            PreLoadCache preLoadCache = bufferCache.values().stream()
                    .filter(p -> p.cache.size() > 0)
                    .findAny().orElse(null);
            if (null != preLoadCache) {
                try {
                    destroyOne(preLoadCache.cache.remove());
                } catch (NoSuchElementException ignored) {
                }
            } else {
                break;
            }
        }

        if (usedSize.get() + size > maxMemorySize) {
            // 如果内存不足，唤醒清理线程立即执行清理
            threads.wakeupThread(EVICT_THREAD);
            // 等待5x10ms，如果还不足抛出异常
            for (int i = 0; i < 5 && usedSize.get() + size > maxMemorySize; i++) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted: ", e);
                }
            }
            if (usedSize.get() + size > maxMemorySize) {
                throw new OutOfMemoryError();
            }
        }
        if (usedSize.addAndGet(size) > maxMemorySize * EVICT_RATIO) {
            threads.wakeupThread(EVICT_THREAD);
        }
    }

    private void releaseIfDirect(ByteBuffer byteBuffer) {
        if(byteBuffer instanceof DirectBuffer) {
            try {
                Method getCleanerMethod;
                getCleanerMethod = byteBuffer.getClass().getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(byteBuffer, new Object[0]);
                cleaner.clean();
            }catch (Exception e) {
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
            if(null != preLoadCache) {
                try {
                    ByteBuffer byteBuffer = preLoadCache.cache.remove();
                    preLoadCache.onFlyCounter.getAndIncrement();
                    return byteBuffer;
                } catch (NoSuchElementException e) {
                    logger.warn("Pool is empty, create ByteBuffer: {}", bufferSize);
                    ByteBuffer byteBuffer = createOne(bufferSize);
                    preLoadCache.onFlyCounter.getAndIncrement();
                    return byteBuffer;
                }
            } else {
                logger.warn("No cached buffer in pool, create ByteBuffer: {}", bufferSize);
                return createOne(bufferSize);

            }
        } catch (OutOfMemoryError outOfMemoryError) {
            logger.debug("OOM: {}/{}.", Format.formatSize(usedSize.get()),Format.formatSize(maxMemorySize));
            throw outOfMemoryError;
        }
    }
    public void releaseDirect(ByteBuffer byteBuffer, BufferHolder bufferHolder) {
        directBufferHolders.remove(bufferHolder);
        int size = byteBuffer.capacity();
        PreLoadCache preLoadCache = bufferCache.get(size);
        if(null != preLoadCache) {
            byteBuffer.clear();
            preLoadCache.cache.add(byteBuffer);
            preLoadCache.onFlyCounter.getAndDecrement();
        } else {
            destroyOne(byteBuffer);
        }
    }


    public void releaseMMap(BufferHolder bufferHolder) {
        mMapBufferHolders.remove(bufferHolder);
        usedSize.getAndAdd(-1 * bufferHolder.size());

    }

    static class PreLoadCache {
        final int bufferSize;
        final int coreCount, maxCount;
        final Queue<ByteBuffer> cache = new ConcurrentLinkedQueue<>();
        final AtomicLong onFlyCounter = new AtomicLong(0L);
        final AtomicInteger referenceCount;
        PreLoadCache(int bufferSize, int coreCount, int maxCount) {
            this.bufferSize = bufferSize;
            this.coreCount = coreCount;
            this.maxCount = maxCount;
            this.referenceCount = new AtomicInteger(1);
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
