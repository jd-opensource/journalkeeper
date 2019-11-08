package io.journalkeeper.core.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public interface ClusterReadyAware {
    /**
     * 等待Leader选举出来
     * @param maxWaitMs 最大等待时间，当maxWaitMs小于等于0时，永远等待，直到集群有新的Leader可用。
     */
    void waitForClusterReady(long maxWaitMs) throws InterruptedException, TimeoutException;
    default void waitForClusterReady() throws InterruptedException {
        try {
            waitForClusterReady(0L);
        } catch (TimeoutException ignored){}
    }
}
