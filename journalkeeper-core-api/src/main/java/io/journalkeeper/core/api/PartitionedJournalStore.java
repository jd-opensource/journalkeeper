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

import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.utils.event.Watchable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Journal Store 客户端。实现：
 * Journal Store API(JK-JS API)
 * 一致性日志接口和事件，兼容openmessaging-storage Minimal API。
 * @author LiYue
 * Date: 2019-04-23
 */
public interface PartitionedJournalStore extends Watchable {
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     * @param partition 分区
     * @param batchSize 日志数量
     * @param entries 待写入的序列化后的日志。
     */
    CompletableFuture<Long> append(int partition, int batchSize, byte [] entries);

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param partition 分区
     * @param batchSize 日志数量
     * @param entries 待写入的序列化后的日志。
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     */
    CompletableFuture<Long> append(int partition, int batchSize, byte [] entries, ResponseConfig responseConfig);

    /**
     * 查询日志
     * @param partition 分区
     * @param index 查询起始位置。
     * @param size 查询条数。
     *
     * @return 读到的日志，返回的数据条数为min(maxIndex - index, size)。
     * @throws IndexOverflowException 参数index必须小于当前maxIndex。
     * @throws IndexUnderflowException 参数index不能小于当前minIndex。
     */
    CompletableFuture<List<RaftEntry>> get(int partition, long index, int size);

    /**
     * 查询每个分区当前最小已提交日志索引序号。
     * @return 每个分区当前最小已提交日志索引序号。
     */
    CompletableFuture<Map<Integer, Long>> minIndices();

    /**
     * 查询每个分区当前最大已提交日志索引序号。
     * @return 每个分区当前最大已提交日志索引序号。
     */
    CompletableFuture<Map<Integer, Long>> maxIndices();

    /**
     * 删除旧日志，只允许删除最旧的部分日志（即增加minIndex，删除之前的日志）。
     * 保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 在集群中复制到大多数节点都完成删除后返回。
     *
     * @param toIndices 所有分区删除日志索引位置，小于这个位置的日志将被删除。
     * @return 所有分区当前最小已提交日志索引位置。
     */
    CompletableFuture compact(Map<Integer, Long> toIndices);


    /**
     * 变更分区。失败抛出异常。
     * @param partitions 变更后的所有分区。
     */
    CompletableFuture scalePartitions(int [] partitions);

    /**
     * 列出当前所有分区，由小到大排序。
     * @return 当前所有分区
     */
    CompletableFuture<int []> listPartitions();
}
