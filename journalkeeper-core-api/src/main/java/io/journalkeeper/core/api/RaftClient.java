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

import io.journalkeeper.base.Queryable;
import io.journalkeeper.core.api.transaction.TransactionalJournalClient;
import io.journalkeeper.utils.event.Watchable;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Raft客户端，实现接口：
 * JournalKeeper RAFT API(JK-RAFT API)
 * Journal Keeper Configuration API（JK-C API）
 * @author LiYue
 * Date: 2019-03-14
 * @param <Q> 状态查询条件类型
 * @param <QR> 状态查询结果类型
 * @param <E> 日志的类型
 * @param <ER> 更新执行结果
 *
 */
public interface RaftClient<E, ER, Q, QR> extends Queryable<Q, QR>, Watchable, ClusterReadyAware, ServerConfigAware, TransactionalJournalClient {


    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作日志数组
     */
    default CompletableFuture<ER> update(E entry) {
        return update(entry, RaftJournal.DEFAULT_PARTITION, 1, false,  ResponseConfig.REPLICATION);
    }

    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作日志数组
     * @param partition 分区
     * @param batchSize 批量大小
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(E entry, int partition, int batchSize, ResponseConfig responseConfig) {
        return update(entry, partition, batchSize, false, responseConfig);
    }

    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作日志数组
     * @param partition 分区
     * @param batchSize 批量大小
     * @param includeHeader entry中是否包含Header
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}时才返回执行结果，否则返回null.
     */
    CompletableFuture<ER> update(E entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig);

    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param query 查询条件
     * @return 查询结果
     */
    @Override
    CompletableFuture<QR> query(Q query);

    void stop();
}
