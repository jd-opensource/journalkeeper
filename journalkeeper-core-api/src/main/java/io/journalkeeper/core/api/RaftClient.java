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
import io.journalkeeper.utils.event.Watchable;

import java.io.Serializable;
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
public interface RaftClient<E, ER, Q, QR> extends Queryable<Q, QR>, Watchable {


    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作日志数组
     */
    CompletableFuture<ER> update(E entry);

    CompletableFuture<ER> update(E entry, int partition, int batchSize, ResponseConfig responseConfig);

    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param query 查询条件
     * @return 查询结果
     */
    @Override
    CompletableFuture<QR> query(Q query);

    //TODO: 把下面这些集群管理和监控的功能拆分出单独的ManagementClient接口

    /**
     * 获取集群配置。
     * @return 所有选民节点、观察者节点和当前的LEADER节点。
     */
    CompletableFuture<ClusterConfiguration> getServers();

    /**
     * 获取集群配置。
     * @return 所有选民节点、观察者节点和当前的LEADER节点。
     */
    CompletableFuture<ClusterConfiguration> getServers(URI uri);

    /**
     * 变更选民节点配置。
     * @param oldConfig 当前配置，用于验证。
     * @param newConfig 变更后的配置
     * @return true：成功，其它：失败。
     */
    CompletableFuture<Boolean> updateVoters(List<URI> oldConfig, List<URI> newConfig);

    /**
     * 转换节点的角色。如果提供的角色与当前角色相同，立即返回。
     * 如果需要转换角色，成功转换之后返回。
     * 转换失败抛出异常。
     * @param uri 节点URI
     * @param roll 新角色
     */
    CompletableFuture convertRoll(URI uri, RaftServer.Roll roll);

    /**
     * 压缩WAL
     * @param toIndices
     * @return
     */
    CompletableFuture<Void> compact(Map<Integer, Long> toIndices);

    CompletableFuture<Void> scalePartitions(int[] partitions);

    CompletableFuture<Long> serverLastApplied(URI uri);
    void stop();

}
