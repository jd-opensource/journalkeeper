package io.journalkeeper.base;

import java.util.concurrent.CompletableFuture;

/**
 * 可查询的
 * @author liyue25
 * Date: 2019-03-14
 * @param <Q> 查询条件
 * @param <R> 查询结果
 */
public interface Queryable<Q, R> {
    /**
     * 查询
     * @param query 查询条件
     * @return 查询结果
     */
    CompletableFuture<R> query(Q query);
}
