package io.journalkeeper.core.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public interface ClusterReadyAware {
    /**
     * 等待Leader选举出来
     * @param maxWaitMs 最大等待时间，当maxWaitMs小于等于0时，永远等待，直到集群有新的Leader可用。
     * @throws java.util.concurrent.TimeoutException 等待超过maxWaitMs，则抛出超时异常。
     */
    CompletableFuture whenClusterReady(long maxWaitMs);

    default void waitForClusterReady(long maxWaitMs) throws ExecutionException, InterruptedException {
        whenClusterReady(maxWaitMs).get();
    }
}
