/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author LiYue
 * Date: 2019-09-09
 */
public interface ClusterReadyAware {
    /**
     * 等待Leader选举出来
     * @param maxWaitMs 最大等待时间，当maxWaitMs小于等于0时，永远等待，直到集群有新的Leader可用时返回。
     * @throws TimeoutException 如果超过最大等待时间仍然没有Leader选举出来，则抛出此异常。
     */
    void waitForClusterReady(long maxWaitMs) throws TimeoutException;

    /**
     * 等待Leader选举出来,直到集群有新的Leader可用时返回。
     */
    default void waitForClusterReady() {
        try {
            waitForClusterReady(0L);
        } catch (TimeoutException ignored){}
    }
}
