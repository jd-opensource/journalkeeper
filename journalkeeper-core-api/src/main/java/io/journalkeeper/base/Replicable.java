package io.journalkeeper.base;

import java.util.concurrent.CompletableFuture;

/**
 * 可复制的
 * @author liyue25
 * Date: 2019-03-14
 */
public interface Replicable<T> {
    /**
     * 复制一份新的状态，用户创建快照
     */
    CompletableFuture<T> replicate(String storageLocation);
}
