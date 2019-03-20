package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.Queryable;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Properties;

/**
 * 状态机
 * 状态数据
 * 状态持久化
 * 对应日志位置
 * 可选实现：java.io.Flushable, java.io.Closable
 * @author liyue25
 * Date: 2019-03-20
 */
public interface State<E, Q, R> extends Queryable<Q, R> {
    /**
     * 在状态state上执行命令entries。要求线性语义和原子性.
     * 成功返回新状态，否则抛异常。
     * @param entry 待执行的命令
     */
    void execute(E entry);

    /**
     * 当前状态对应的日志位置
     * lastApplied
     */
    long lastApplied();

    void init(Path path, Properties properties);

    /**
     * 将状态物理复制一份，保存到path
     */
    State<E, Q, R> takeASnapshot(Path path);

    /**
     * 将所有持久化的状态复制序列化成多个字节数组的迭代器，便于网络传输。
     */
    Iterator<ByteBuffer> serialize();

    /**
     * 恢复状态。
     * 反复调用install复制序列化的状态数据。
     * 所有数据都复制完成后，最后调用installFinish恢复状态。
     * @param data 日志数据片段
     */
    void install(ByteBuffer data);
    void installFinish(long lastApplied);
}
