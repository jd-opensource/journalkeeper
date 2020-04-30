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
package io.journalkeeper.persistence.local.cache;

import io.journalkeeper.utils.format.Format;
import io.journalkeeper.utils.spi.Singleton;
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
@Singleton
public class PreloadBufferPool implements MemoryCacheManager {
    private static final Logger logger = LoggerFactory.getLogger(PreloadBufferPool.class);
    private static final String PRELOAD_THREAD = "PreloadBuffer-PreloadThread";
    private static final String EVICT_THREAD = "PreloadBuffer-EvictThread";
    // 缓存比率：如果非堆内存使用率超过这个比率，就不再申请内存，抛出OOM。
    // 由于jvm在读写文件的时候会用到少量DirectBuffer作为缓存，必须预留一部分。
    private final static double DEFAULT_CACHE_RATIO = 0.9d;
    /**
     * 缓存清理比率阈值，超过这个阈值执行缓存清理。
     */
    private static final float DEFAULT_EVICT_RATIO = 0.9f;
    /**
     * 缓存核心利用率，系统会尽量将这个比率以内的内存用满。
     */
    private static final float DEFAULT_CORE_RATIO = 0.8f;
    private static final long DEFAULT_WRITE_PAGE_EXTRA_WEIGHT_MS = 60000L;
    private final static long INTERVAL_MS = 50L;

    private final static String MAX_MEMORY_KEY = "memory_cache.max_memory";
    private final static String EVICT_RATIO_KEY = "memory_cache.evict_ratio";
    private final static String CORE_RATIO_KEY = "memory_cache.core_ratio";
    private static final String WRITE_PAGE_EXTRA_WEIGHT_MS_KEY="memory_cache.write.weight.ms";

    private static final PreloadBufferPool instance = null;
    private final Threads threads = ThreadsFactory.create();
    // 可用的堆外内存上限，这个上限可以用JVM参数"PreloadBufferPool.MaxMemory"指定，
    // 默认为虚拟机最大内存（VM.maxDirectMemory()）的90%。
    private final long maxMemorySize;
    // 核心堆外内存大小，JournalKeeper总是尽量占满coreMemorySize内存用于缓存更多的文件，提升读写性能。
    private final long coreMemorySize;
    // 堆外内存超过evictMemorySize就会启动清理，清理的策略是LRU
    private final long evictMemorySize;

    // 正在写入的页在置换时有额外的权重，这个权重用时间Ms体现。
    // 默认是60秒。
    // 置换权重 = 上次访问时间戳 + 额外权重，优先从内存中驱逐权重小的页。
    // 例如：一个只读的页，上次访问时间戳是T，一个读写页，上次访问时间是T - 60秒，
    // 这两个页在置换时有同样的权重
    private final long writePageExtraWeightMs;
    private final AtomicLong usedSize = new AtomicLong(0L);
    private final Set<BufferHolder> directBufferHolders = ConcurrentHashMap.newKeySet();
    private final Set<BufferHolder> mMapBufferHolders = ConcurrentHashMap.newKeySet();
    private Map<Integer, PreLoadCache> bufferCache = new ConcurrentHashMap<>();

    public PreloadBufferPool() {

        maxMemorySize = calcMaxMemorySize();
        float evictRatio = getFloatProperty(EVICT_RATIO_KEY, DEFAULT_EVICT_RATIO);
        evictMemorySize = Math.round(maxMemorySize * evictRatio);
        float coreRatio = getFloatProperty(CORE_RATIO_KEY, DEFAULT_CORE_RATIO);
        coreMemorySize = Math.round(maxMemorySize * coreRatio);
        writePageExtraWeightMs = Long.parseLong(System.getProperty(WRITE_PAGE_EXTRA_WEIGHT_MS_KEY,String.valueOf(DEFAULT_WRITE_PAGE_EXTRA_WEIGHT_MS)));

        threads.createThread(buildPreloadThread());
        threads.createThread(buildEvictThread());
        threads.start();

        logger.info("JournalKeeper PreloadBufferPool loaded, max direct memory: {}, core direct memory: {}, evict direct memory: {}.",
                Format.formatSize(maxMemorySize),
                Format.formatSize(coreMemorySize),
                Format.formatSize(evictMemorySize));
    }

    private static float getFloatProperty(String key, float defaultValue) {
        try {
            return Float.parseFloat(System.getProperty(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
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
                .condition(() -> usedSize.get() > evictMemorySize)
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
                    .map(bufferHolder -> new LruWrapper<>(bufferHolder, bufferHolder.lastAccessTime(), bufferHolder.writable() ? writePageExtraWeightMs : 0L))
                    .sorted(Comparator.comparing(LruWrapper::getWeight))
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

    @Override
    public void printMetric() {
        long totalUsed = usedSize.get();
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
        long mmpUsed = mMapBufferHolders.stream().mapToLong(BufferHolder::size).sum();
        long directUsed = directBufferHolders.stream().mapToLong(BufferHolder::size).sum();
        logger.info("Direct memory usage: preload/direct/mmp/used/max: {}/{}/{}/{}/{}.",
                Format.formatSize(plUsed),
                Format.formatSize(directUsed),
                Format.formatSize(mmpUsed),
                Format.formatSize(totalUsed),
                Format.formatSize(maxMemorySize));
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

    @Override
    public synchronized void addPreLoad(int bufferSize, int coreCount, int maxCount) {
        PreLoadCache preLoadCache = bufferCache.putIfAbsent(bufferSize, new PreLoadCache(bufferSize, coreCount, maxCount));
        if (null != preLoadCache) {
            preLoadCache.referenceCount.incrementAndGet();
        }
    }

    @Override
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
                            .map(bufferHolder -> new LruWrapper<>(bufferHolder, bufferHolder.lastAccessTime(), bufferHolder.writable() ? writePageExtraWeightMs : 0L))
                            .sorted(Comparator.comparing(LruWrapper::getWeight))
                            .collect(Collectors.toList());
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

    @Override
    public void allocateMMap(BufferHolder bufferHolder) {
        reserveMemory(bufferHolder.size());
        mMapBufferHolders.add(bufferHolder);
    }

    @Override
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

    @Override
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


    @Override
    public void releaseMMap(BufferHolder bufferHolder) {
        mMapBufferHolders.remove(bufferHolder);
        usedSize.getAndAdd(-1 * bufferHolder.size());

    }

    @Override
    public Collection<PreloadCacheMetric> getCaches() {
        return new ArrayList<>(bufferCache.values());
    }

    @Override
    public long getMaxMemorySize() {
        return maxMemorySize;
    }

    @Override
    public long getTotalUsedMemorySize() {
        return usedSize.get();
    }

    @Override
    public long getDirectUsedMemorySize() {
        return directBufferHolders.stream().mapToLong(BufferHolder::size).sum() +
                bufferCache.values().stream().mapToLong(c -> c.getBufferSize() * c.getCachedCount()).sum();
    }


    /**
     * 计算可供缓存使用的最大堆外内存。
     *
     * 1. 如果PreloadBufferPool.MaxMemory设置为数值，直接使用设置值。
     * 2. 如果PreloadBufferPool.MaxMemory设置为百分比，比如：90%，最大堆外内存 = 物理内存 * 90% - 最大堆内存（由JVM参数-Xmx配置）
     * 3. 如果PreloadBufferPool.MaxMemory未设置或者设置了非法值，最大堆外内存 = VM.maxDirectMemory() * 90%。
     * 其中VM.maxDirectMemory()取值为JVM参数-XX:MaxDirectMemorySize，如果未设置-XX:MaxDirectMemorySize，取值为JVM参数-Xmx。
     *
     * @return 可使用的最大堆外内存大小。
     */
    private long calcMaxMemorySize() {
        String mmsString = System.getProperty(MAX_MEMORY_KEY);
        int pct = Format.getPercentage(mmsString);
        if (pct > 0 && pct < 100) {
            long physicalMemorySize = getPhysicalMemorySize();
            long reservedHeapMemorySize = Runtime.getRuntime().maxMemory();
            if (Long.MAX_VALUE == reservedHeapMemorySize) {
                logger.warn("Runtime.getRuntime().maxMemory() returns unlimited!");
                reservedHeapMemorySize = physicalMemorySize / 2;
            }
            // 如果设置了百分比，最大可使用堆外内存= 物理内存 * 百分比 - 最大堆内存
            long mms = physicalMemorySize * pct / 100 - reservedHeapMemorySize;
            if (mms > 0) {
                return mms;
            } else {
                return Math.round(VM.maxDirectMemory() * DEFAULT_CACHE_RATIO);
            }
        }
        return Format.parseSize(System.getProperty(MAX_MEMORY_KEY), Math.round(VM.maxDirectMemory() * DEFAULT_CACHE_RATIO));
    }

    private static long getPhysicalMemorySize() {
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        return os.getTotalPhysicalMemorySize();
    }

    @Override
    public void close() {
        if (null != PreloadBufferPool.instance) {
            PreloadBufferPool.instance.threads.stop();
            PreloadBufferPool.instance.bufferCache.values().forEach(p -> {
                while (!p.cache.isEmpty()) {
                    PreloadBufferPool.instance.destroyOne(p.cache.remove());

                }
            });
            PreloadBufferPool.instance.directBufferHolders.parallelStream().forEach(BufferHolder::evict);
            PreloadBufferPool.instance.mMapBufferHolders.parallelStream().forEach(BufferHolder::evict);
            PreloadBufferPool.instance.bufferCache.values().forEach(p -> {
                while (!p.cache.isEmpty()) {
                    PreloadBufferPool.instance.destroyOne(p.cache.remove());

                }
            });
        }
        PreloadBufferPool.logger.info("Preload buffer pool closed.");
    }
    @Override
    public long getMapUsedMemorySize() {
        return mMapBufferHolders.stream().mapToLong(BufferHolder::size).sum();
    }

    public static class PreLoadCache implements PreloadCacheMetric {
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
        private final long extraWeight;
        private final V t;

        LruWrapper(V t, long lastAccessTime, long extraWeight) {
            this.lastAccessTime = lastAccessTime;
            this.t = t;
            this.extraWeight = extraWeight;
        }

        private long getLastAccessTime() {
            return lastAccessTime;
        }

        private V get() {
            return t;
        }

        private long getWeight() {
            return lastAccessTime + extraWeight;
        }
    }
}
