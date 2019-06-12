package com.jd.journalkeeper.utils.event;

import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.threads.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 事件总线，缓存事件，支持推拉2种模型：push和pull。
 * push适用于本地监听，当有事件产生时直接触发事件监听器 {@link EventWatcher}。
 * push模式的流程：
 * 1. 客户端调用 {@link #watch(EventWatcher)}添加一个监听器；
 * 2. 当有事件产生时直接触发事件监听器 {@link EventWatcher}；
 * 3. 客户端调用 {@link #unWatch(EventWatcher)} 删除监听器，不再监听事件。
 *
 * pull模式下，客户端需要主动拉取事件，一般用于远程监听。
 * 1. 首先客户端调用 {@link #addPullWatch()} 创建一个监听，返回监听ID；
 * 2. 客户端调用 {@link #pullIntervalMs()} 获取pull间隔时间；
 * 3. 客户端启动一个定时器，每隔{@link #pullIntervalMs()}时间，调用 {@link #pullEvents(long)}拉取事件；
 * 4. 客户端收到事件后，调用 {@link #ackPullEvents(long, long)}  确认。
 * 5. 重复步骤4，直到调用 {@link #removePullWatch(long)} 取消订阅。
 *
 * 注意：客户端需要按照服务端给出的时间间隔拉取事件，如果客户端长时间不来拉取事件，服务端将认为客户端已经宕机，自动取消订阅。
 *
 * @author liyue25
 * Date: 2019-04-12
 */
public class EventBus implements Watchable {
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);
    private final NavigableMap<Long, Event> cachedEvents = new ConcurrentSkipListMap<>();
    private final ExecutorService eventWatcherExecutor;
    private final AtomicLong watchIdGenerator = new AtomicLong(0L);
    private final AtomicLong nextSequence = new AtomicLong(0L);
    private final Set<EventWatcher> eventWatchers = ConcurrentHashMap.newKeySet();
    private final Map<Long, PullEventWatcher> pullEventWatchers = new ConcurrentHashMap<>();
    private final long pullEventIntervalMs;
    private final long pullEventWatcherTimeout;
    private final LoopThread removeTimeoutPullWatchersThread;
    private final Collection<EventInterceptor> interceptors;

    public EventBus(ExecutorService eventWatcherExecutor, long pullEventIntervalMs) {
        this.eventWatcherExecutor = eventWatcherExecutor;
        this.pullEventIntervalMs = pullEventIntervalMs;
        this.pullEventWatcherTimeout = 5 * pullEventIntervalMs;
        interceptors = ServiceSupport.loadAll(EventInterceptor.class);
        this.removeTimeoutPullWatchersThread = buildRemoveTimeoutPullWatchersThread();
        this.removeTimeoutPullWatchersThread.start();
    }

    public EventBus() {
        this(Executors.newSingleThreadExecutor(), 1000L);
    }

    private LoopThread buildRemoveTimeoutPullWatchersThread() {
        return LoopThread.builder()
                .name("RemoveTimeoutPullWatchersThread")
                .doWork(this::removeTimeoutPullWatchers)
                .sleepTime(pullEventWatcherTimeout, pullEventWatcherTimeout)
                .onException(e -> logger.warn("RemoveTimeoutPullWatchersThread Exception: ", e))
                .daemon(true)
                .build();
    }

    private void removeTimeoutPullWatchers() {
        // TODO 多次超时才移除
//        pullEventWatchers.entrySet().removeIf(entry -> entry.getValue().lastPullTimestamp + pullEventWatcherTimeout < System.currentTimeMillis());
        pullEventWatchers.entrySet().removeIf(entry -> entry.getValue().lastPullTimestamp + pullEventWatcherTimeout * 3 < System.currentTimeMillis());
    }

    /**
     * 触发一个事件
     * @param event 事件
     */
    public synchronized void fireEvent(Event event) {
        for(EventInterceptor interceptor : interceptors) {
            if(!interceptor.onEvent(event, this)) {
                logger.info("Event canceled by an interceptor, type: {}, data: {}"
                        , event.getEventType(),event.getEventData());
                return;
            }
        }
        // 回调Push eventWatchers
        eventWatchers.forEach(eventWatcher -> eventWatcherExecutor.submit(() -> eventWatcher.onEvent(event)));

        if(!pullEventWatchers.isEmpty()) {
            cachedEvents.put(nextSequence.getAndIncrement(), event);
        }
    }

    /**
     * 添加事件监听器，当事件发生时会调用监听器
     * @param eventWatcher 事件监听器
     */
    @Override
    public void watch(EventWatcher eventWatcher) {
        if(eventWatcher != null) {
            eventWatchers.add(eventWatcher);
        }
    }

    /**
     * 删除事件监听器
     * @param eventWatcher 事件监听器
     */
    @Override
    public void unWatch(EventWatcher eventWatcher) {
        if(eventWatcher != null) {
            eventWatchers.remove(eventWatcher);
        }
    }

    /**
     * 添加pull模式事件监听。
     * @return 监听ID
     */
    public long addPullWatch() {
        long pullWatchId = watchIdGenerator.getAndIncrement();
        pullEventWatchers.put(pullWatchId, new PullEventWatcher(nextSequence.get()));
        return pullWatchId;
    }

    /**
     * 删除pull事件监听。
     * @param pullWatchId 监听ID
     */
    public void removePullWatch(long pullWatchId) {
        pullEventWatchers.remove(pullWatchId);
    }

    /**
     * 获取拉取监听事件的时间间隔。
     * @return 监听时间间隔，单位毫秒。
     */
    public long pullIntervalMs() {
        return pullEventIntervalMs;
    }

    /**
     * 拉取事件
     * @param pullWatchId 监听ID
     * @return 从上次ack 的序号至今的所有事件，保证事件有序。
     * 如果没有事件返回长度为0的List。
     * 如果监听ID {@code pullWatchId} 不存在，返回null。
     */
    public List<PullEvent> pullEvents(long pullWatchId) {
        PullEventWatcher pullEventWatcher = pullEventWatchers.get(pullWatchId);
        if(null != pullEventWatcher) {
            List<PullEvent> pullEvents = cachedEvents.tailMap(pullEventWatcher.sequence.get())
                    .entrySet().stream()
                    .map(entry ->
                            new PullEvent(entry.getValue().getEventType(),
                                    entry.getKey(),
                                    entry.getValue().getEventData()
                                    ))
                    .collect(Collectors.toList());
            pullEventWatcher.touch();
            return pullEvents;
        }
        return null;
    }

    /**
     * 确认事件。拉取成功后，调用此方法确认。
     * @param pullWatchId 监听ID
     * @param sequence 上次拉取的事件中最后一条事件的sequence。
     */
    public void ackPullEvents(long pullWatchId, long sequence) {
        PullEventWatcher pullEventWatcher = pullEventWatchers.get(pullWatchId);
        if(null != pullEventWatcher && pullEventWatcher.sequence.get() > sequence) {
            pullEventWatcher.sequence.set(sequence);
        }
    }


    private static class PullEventWatcher {
        PullEventWatcher(long sequence) {
            this.sequence.set(sequence);
        }
        private final AtomicLong sequence = new AtomicLong(0L);
        private long lastPullTimestamp = System.currentTimeMillis();

        void touch() {
            lastPullTimestamp = System.currentTimeMillis();
        }
    }

    public void shutdown() {
        removeTimeoutPullWatchersThread.stop();
    }

    public boolean hasEventWatchers() {
        return !eventWatchers.isEmpty();
    }
}
