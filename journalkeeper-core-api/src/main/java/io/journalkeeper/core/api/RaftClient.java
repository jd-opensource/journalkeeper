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

import io.journalkeeper.core.api.transaction.TransactionClient;
import io.journalkeeper.utils.event.Watchable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 * JournalKeeper RAFT API(JK-RAFT API)
 * Raft客户端。
 *
 * {@link #update(java.util.List, boolean, io.journalkeeper.core.api.ResponseConfig)}方法用于写入操作命令，每条操作命令就是日志中的一个条目，
 * 操作命令在安全写入日志后，将在状态机中执行，
 * 执行的结果将会更新状态机中的状态。
 *
 * {@link #query(Object)}方法用于查询状态机中的数据。
 *
 * 例如，状态机是一个关系型数据库（RDBMS），数据库中的数据就是“状态”，或者成为状态数据。
 * 在这里，可以把更新数据的SQL（UPDATE，INSERT, ALTER 等）作为操作命令，调用{@link #update(java.util.List, boolean, io.journalkeeper.core.api.ResponseConfig)}方法
 * 去在JournalKeeper集群中执行。JournalKeeper将操作命令复制到集群的每个节点上，
 * 并且可以保证在每个节点，用一样的顺序去执行这些SQL，更新每个节点的数据库，
 * 当这些操作命令都在每个节点的数据库中执行完成后，
 * 这些节点的数据库中必然有相同的数据。
 *
 * 如果需要查询数据库中的数据，可以把查询SQL（SELECT）作为查询条件，
 * 调用{@link #query(Object)}方法，JournalKeeper将查询命令发送给当前LEADER节点的状态机，
 * 也就是数据库，去执行查询SQL语句，然后将结果返回给客户端。
 *
 * @author LiYue
 * Date: 2019-03-14
 * @param <Q> 状态查询条件类型
 * @param <QR> 状态查询结果类型
 * @param <E> 操作命令的类型
 * @param <ER> 状态机执行结果类型
 *
 */
public interface RaftClient<E, ER, Q, QR> extends Watchable, ClusterReadyAware, ServerConfigAware, TransactionClient<E> {

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作命令数组
     * @return 操作命令在状态机的执行结果
     */
    default CompletableFuture<ER> update(E entry) {
        return update(entry, RaftJournal.DEFAULT_PARTITION, 1, false,  ResponseConfig.REPLICATION);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * @param entry 操作命令数组
     * @param partition 分区
     * @param batchSize 批量大小
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(E entry, int partition, int batchSize, ResponseConfig responseConfig) {
        return update(entry, partition, batchSize, false, responseConfig);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * @param entry 操作命令数组
     * @param partition 分区
     * @param batchSize 批量大小
     * @param includeHeader entry中是否包含Header
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(E entry, int partition, int batchSize, boolean includeHeader, ResponseConfig responseConfig) {
        return update(new UpdateRequest<>(entry, partition, batchSize), includeHeader, responseConfig);
    }


    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * @param updateRequest See {@link UpdateRequest}
     * @param includeHeader entry中是否包含Header
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(UpdateRequest<E> updateRequest, boolean includeHeader, ResponseConfig responseConfig){
        return update(Collections.singletonList(updateRequest), includeHeader, responseConfig)
                .thenApply(ers -> {
                    if(null != ers && ers.size() > 0) {
                        return ers.get(0);
                    }
                    return null;
                });
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * 此方法等效于：update(updateRequest, false, responseConfig);
     * @param updateRequest See {@link UpdateRequest}
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(UpdateRequest<E> updateRequest, ResponseConfig responseConfig) {
        return update(updateRequest, false, responseConfig);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * 此方法等效于：update(updateRequest, false, ResponseConfig.REPLICATION);
     * @param updateRequest See {@link UpdateRequest}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<ER> update(UpdateRequest<E> updateRequest) {
        return update(updateRequest, false, ResponseConfig.REPLICATION);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 此方法等效于：update(updateRequests, false, responseConfig);
     * @param updateRequests See {@link UpdateRequest}
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<List<ER>> update(List<UpdateRequest<E>> updateRequests, ResponseConfig responseConfig) {
        return update(updateRequests, false, responseConfig);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * 此方法等效于：update(updateRequests, false, ResponseConfig.REPLICATION);
     * @param updateRequests See {@link UpdateRequest}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    default CompletableFuture<List<ER>> update(List<UpdateRequest<E>> updateRequests) {
        return update(updateRequests, false, ResponseConfig.REPLICATION);
    }

    /**
     * 写入操作命令变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * @param updateRequests See {@link UpdateRequest}
     * @param includeHeader entry中是否包含Header
     * @param responseConfig 响应级别。See {@link ResponseConfig}
     * @return 操作执行结果。只有响应级别为{@link ResponseConfig#REPLICATION}或者{@link ResponseConfig#ALL}时才返回执行结果，否则返回null.
     */
    CompletableFuture<List<ER>> update(List<UpdateRequest<E>> updateRequests, boolean includeHeader, ResponseConfig responseConfig);


    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param query 查询条件
     * @return 查询结果
     */
    CompletableFuture<QR> query(Q query);

    void stop();
}
