package io.journalkeeper.core.api;

import io.journalkeeper.base.Replicable;

import java.util.concurrent.CompletableFuture;

/**
 * 状态机
 *
 * @param <E> 日志的类型
 * @param <S> 状态类型
 * @author liyue25
 * Date: 2019-03-14
 */
public interface StateMachine<E,  S extends Replicable<S>> {

    /**
     * 在状态state上依次执行命令entries。要求线性语义和原子性
     * @param state 初始状态
     * @param entries 待执行的命令数组
     * @return 成功返回新状态，否则抛异常。
     */
    CompletableFuture<S> execute(S state, E... entries);
}
