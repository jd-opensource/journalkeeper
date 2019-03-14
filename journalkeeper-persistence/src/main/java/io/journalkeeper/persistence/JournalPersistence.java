package io.journalkeeper.persistence;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * 日志持久化接口
 * @author liyue25
 * Date: 2019-03-14
 */
public interface JournalPersistence {
    /**
     * 最小位置，初始化为0
     */
    long min();

    /**
     * 最大位置， 初始化为0
     */
    long max();

    /**
     * 截断最新的日志
     * @param givenMax 新的最大位置，不能大于当前的最大位置
     */
    CompletableFuture<Void> truncate(long givenMax);

    /**
     * 删除旧日志。考虑到大多数基于文件的实现做到精确按位置删除代价较大，
     * 不要求精确删除到给定位置。但不能删除给定位置之后的数据。
     * @param givenMin 给定删除位置，这个位置之前都可以删除。
     * @return 删除后当前最小位置。
     */
    CompletableFuture<Long> shrink(long givenMin);

    /**
     * 追加写入
     * @param byteBuffers 待写入的内容
     * @return 写入后新的位置
     */
    CompletableFuture<Long> append(ByteBuffer... byteBuffers);

    /**
     * 读取数据
     * @param position 起始位置
     * @param length 读取长度
     * @return 存放数据的ByteBuffer
     */
    CompletableFuture<ByteBuffer> read(long position, int length);
}
