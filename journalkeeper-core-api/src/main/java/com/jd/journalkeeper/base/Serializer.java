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
    int sizeOf(E journalEntry);

    /**
     * 将日志序列化
     * @param entry 日志
     * @param destBuffer 序列化后写入的buffer
     */
    void serialize(ByteBuffer destBuffer, E entry);

    /**
     * 反序列化日志
     * @param byteBuffer 存放序列化日志的buffer
     * @return 解析出来的日志
     */
    E parse(ByteBuffer byteBuffer);
}
