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
    default CompletableFuture<Long> append(int partition, int batchSize, byte [] entries) {
        return append(partition, batchSize, entries, ResponseConfig.REPLICATION);
    }

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param partition 分区
     * @param batchSize 日志数量
     * @param entries 待写入的序列化后的日志。
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     */
    default CompletableFuture<Long> append(int partition, int batchSize, byte [] entries, ResponseConfig responseConfig) {
        return append(partition, batchSize, entries, false, responseConfig);
    }
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param partition 分区
     * @param batchSize 日志数量
     * @param entries 待写入的序列化后的日志。
     * @param includeHeader 序列化的日志中是否包含header
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     */
    default CompletableFuture<Long> append(int partition, int batchSize, byte [] entries, boolean includeHeader, ResponseConfig responseConfig) {
        return append(new UpdateRequest<>(entries, partition, batchSize), includeHeader, responseConfig);
    }


    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     * @param updateRequest See {@link UpdateRequest}
     * @return 写入日志在分区上的索引序号
     */
    default CompletableFuture<Long> append(UpdateRequest<byte []> updateRequest) {
        return append(updateRequest, ResponseConfig.REPLICATION);
    }

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequest See {@link UpdateRequest}
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 写入日志在分区上的索引序号
     */
    default CompletableFuture<Long> append(UpdateRequest<byte []> updateRequest, ResponseConfig responseConfig) {
        return append(updateRequest, false, responseConfig);
    }
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequest See {@link UpdateRequest}
     * @param includeHeader 序列化的日志中是否包含header
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 写入日志在分区上的索引序号
     */
    CompletableFuture<Long> append(UpdateRequest<byte []> updateRequest, boolean includeHeader, ResponseConfig responseConfig);
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     * @param updateRequests See {@link UpdateRequest}
     * @return 写入日志在分区上的索引序号列表
     */
    default CompletableFuture<List<Long>> append(List<UpdateRequest<byte []>> updateRequests) {
        return append(updateRequests, ResponseConfig.REPLICATION);
    }

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequests See {@link UpdateRequest}
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 写入日志在分区上的索引序号列表
     */
    default CompletableFuture<List<Long>> append(List<UpdateRequest<byte []>> updateRequests, ResponseConfig responseConfig) {
        return append(updateRequests, false, responseConfig);
    }
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequests See {@link UpdateRequest}
     * @param includeHeader 序列化的日志中是否包含header
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 写入日志在分区上的索引序号列表
     */
    CompletableFuture<List<Long>> append(List<UpdateRequest<byte []>> updateRequests, boolean includeHeader, ResponseConfig responseConfig);

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
    CompletableFuture<List<JournalEntry>> get(int partition, long index, int size);

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
     * 列出当前所有分区，由小到大排序。
     * @return 当前所有分区
     */
    CompletableFuture<int []> listPartitions();

    /**
     * 根据JournalEntry存储时间获取索引。
     * @param partition 分区
     * @param timestamp 查询时间，单位MS
     * @return 如果找到，返回最后一条 “存储时间 <= timestamp” JournalEntry的索引。
     * 如果查询时间 < 第一条JournalEntry的时间，返回第一条JournalEntry；
     * 如果找到的JournalEntry前后有多条时间相同的JournalEntry，则返回这些JournalEntry中的的第一条；
     * 其它情况，返回负值。
     */
    CompletableFuture<Long> queryIndex(int partition, long timestamp);
}
