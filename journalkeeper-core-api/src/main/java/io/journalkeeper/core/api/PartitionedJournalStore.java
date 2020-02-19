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
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Partitioned Journal Store。实现：Journal Store API(JK-JS API)
 * Partitioned Journal Store 维护一个多分区、高可靠、高可用、强一致的、分布式WAL日志。
 * WAL日志具有如下特性：
 * <ul>
 *     <li>尾部追加：只能在日志尾部追加写入条目</li>
 *     <li>不可变：日志写入成功后不可修改，不可删除</li>
 *     <li>严格顺序：日志严格按照写入的顺序排列</li>
 *     <li>线性写入：数据写入是线性的，任一时间只能有一个客户端写入。</li>
 * </ul>
 *
 * JournalKeeper支持将一个Journal Store划分为多个逻辑分区，
 * 每个分区都是一个WAL日志，分区间可以并行读写。
 *
 * @author LiYue
 * Date: 2019-04-23
 */
public interface PartitionedJournalStore extends Watchable {

    /**
     * 写入日志。
     * 日志在集群中被复制到大多数节点后返回。
     * @param partition 分区
     * @param batchSize 日志数量
     * @param entries 待写入的序列化后的日志。
     * @return 已写入的日志在分区上的索引序号。
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
     * @return 已写入的日志在分区上的索引序号。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
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
     * @return 已写入的日志在分区上的索引序号。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
     */
    default CompletableFuture<Long> append(int partition, int batchSize, byte [] entries, boolean includeHeader, ResponseConfig responseConfig) {
        return append(new UpdateRequest(entries, partition, batchSize), includeHeader, responseConfig);
    }


    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     * @param updateRequest See {@link UpdateRequest}
     * @return 已写入的日志在分区上的索引序号。
     */
    default CompletableFuture<Long> append(UpdateRequest updateRequest) {
        return append(updateRequest, ResponseConfig.REPLICATION);
    }

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequest See {@link UpdateRequest}
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 已写入的日志在分区上的索引序号。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
     */
    default CompletableFuture<Long> append(UpdateRequest updateRequest, ResponseConfig responseConfig) {
        return append(updateRequest, false, responseConfig);
    }
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequest See {@link UpdateRequest}
     * @param includeHeader 序列化的日志中是否包含header
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 已写入的日志在分区上的索引序号。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
     */
    CompletableFuture<Long> append(UpdateRequest updateRequest, boolean includeHeader, ResponseConfig responseConfig);
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中被复制到大多数节点后返回。
     * @param updateRequests See {@link UpdateRequest}
     * @return 写入日志在分区上的索引序号列表
     */
    default CompletableFuture<List<Long>> append(List<UpdateRequest> updateRequests) {
        return append(updateRequests, ResponseConfig.REPLICATION);
    }

    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequests See {@link UpdateRequest}
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 已写入的日志在分区上的索引序号列表。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
     */
    default CompletableFuture<List<Long>> append(List<UpdateRequest> updateRequests, ResponseConfig responseConfig) {
        return append(updateRequests, false, responseConfig);
    }
    /**
     * 写入日志。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * @param updateRequests See {@link UpdateRequest}
     * @param includeHeader 序列化的日志中是否包含header
     * @param responseConfig 返回响应的配置。See {@link ResponseConfig}
     * @return 已写入的日志在分区上的索引序号列表。注意：由于日志的分区索引序号是在日志提交阶段构建的，
     * 因此只有responseConfig为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时，才会返回分区索引序号。其它responseConfig时，返回null。
     */
    CompletableFuture<List<Long>> append(List<UpdateRequest> updateRequests, boolean includeHeader, ResponseConfig responseConfig);

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
     * 列出当前所有分区。
     * @return 当前所有分区
     */
    CompletableFuture<Set<Integer>> listPartitions();

    /**
     * 根据JournalEntry存储时间获取索引。
     * @param partition 分区
     * @param timestamp 查询时间，单位MS
     * @return 如果找到，返回最后一条 “存储时间 不大于 timestamp” JournalEntry的索引。
     * 如果查询时间 小于 第一条JournalEntry的时间，返回第一条JournalEntry；
     * 如果找到的JournalEntry前后有多条时间相同的JournalEntry，则返回这些JournalEntry中的的第一条；
     * 其它情况，返回负值。
     */
    CompletableFuture<Long> queryIndex(int partition, long timestamp);
}
