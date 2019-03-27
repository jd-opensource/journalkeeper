package com.jd.journalkeeper.base;

/**
 * 序列化接口
 * @author liyue25
 * Date: 2019-03-14
 */
public interface Serializer<T> {
    /**
     * 计算实体序列化后的长度
     * @param t 实体
     * @return 日志序列化后的长度，单位字节。
     */
    int sizeOf(T t);

    /**
     * 将日志序列化
     * @param entry 日志
     */
    byte [] serialize(T entry);

    /**
     * 反序列化日志
     * @param bytes 存放序列化日志的buffer
     * @return 解析出来的日志
     */
    T parse(byte [] bytes);
}
