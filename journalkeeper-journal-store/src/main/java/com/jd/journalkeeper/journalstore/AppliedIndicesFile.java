package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.utils.files.DoubleCopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author liyue25
 * Date: 2019-05-20
 */
public class AppliedIndicesFile extends DoubleCopy implements Map<Integer, Long>{
    private Map<Integer, Long> appliedIndices = new ConcurrentHashMap<>();
    private final AtomicLong version = new AtomicLong(0L);
    private long flushVersion = 0L;
    /**
     * 构造函数
     *
     * @param file        本地存储文件
     */
    public AppliedIndicesFile(File file) throws IOException {
        super(file, Short.BYTES + Short.MAX_VALUE * (Integer.BYTES + Long.BYTES));
    }

    @Override
    protected String getName() {
        return "applied_indices";
    }

    @Override
    protected byte[] serialize() {
        Map<Integer, Long> clone = new HashMap<>(appliedIndices);
        byte [] bytes = new byte[Short.BYTES + clone.size() * (Integer.BYTES + Long.BYTES)];

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putShort((short) clone.size());
        clone.forEach((k, v) -> {
            buffer.putInt(k);
            buffer.putLong(v);
        });

        return bytes;
    }

    @Override
    protected void parse(byte[] data) {

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();

        Map<Integer, Long> clone = new HashMap<>(size);
        while (buffer.hasRemaining()) {
            clone.put(buffer.getInt(), buffer.getLong());
        }

        appliedIndices.clear();
        appliedIndices.putAll(clone);
        flushVersion = version.get();
    }

    @Override
    public synchronized void flush() {
        long currentVersion = version.get();
        if(flushVersion < currentVersion) {
            super.flush();
            flushVersion = currentVersion;
        }
    }

    @Override
    public int size() {
        return appliedIndices.size();
    }

    @Override
    public boolean isEmpty() {
        return appliedIndices.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return appliedIndices.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return appliedIndices.containsValue(value);
    }

    @Override
    public Long get(Object key) {
        return appliedIndices.get(key);
    }

    @Override
    public Long put(Integer key, Long value) {
        version.incrementAndGet();
        return appliedIndices.put(key, value);
    }

    @Override
    public Long remove(Object key) {
        version.incrementAndGet();
        return appliedIndices.remove(key);
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Long> m) {
        version.incrementAndGet();
        appliedIndices.putAll(m);
    }

    @Override
    public void clear() {
        version.incrementAndGet();
        appliedIndices.clear();
    }

    @Override
    public Set<Integer> keySet() {
        return appliedIndices.keySet();
    }

    @Override
    public Collection<Long> values() {
        return appliedIndices.values();
    }

    @Override
    public Set<Entry<Integer, Long>> entrySet() {
        return appliedIndices.entrySet();
    }
}
