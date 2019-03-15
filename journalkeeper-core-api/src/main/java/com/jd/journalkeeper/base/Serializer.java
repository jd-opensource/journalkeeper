package com.jd.journalkeeper.base;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 日志序列化接口
 * @author liyue25
 * Date: 2019-03-14
 */
public interface Serializer<E> {
    /**
     * 计算日志序列化后的长度
     * @param journalEntry 日志
     * @return 日志序列化后的长度，单位字节。
     */
    long sizeOf(E journalEntry);

    /**
     * 将日志序列化
     * @param journalEntries 日志
     * @param destBuffer 序列化后写入的buffer
     */
    void serialize(ByteBuffer destBuffer, E... journalEntries);

    /**
     * 反序列化日志
     * @param byteBuffer 存放序列化日志的buffer
     * @param maxSize 最多序列化条数
     * @return 解析出来的日志
     */
    List<E> parse(ByteBuffer byteBuffer, long maxSize);
}
