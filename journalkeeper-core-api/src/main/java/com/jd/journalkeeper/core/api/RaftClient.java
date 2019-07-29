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
package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.Queryable;
import com.jd.journalkeeper.utils.event.EventWatcher;
import com.jd.journalkeeper.utils.event.Watchable;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * Raft客户端，实现接口：
 * JournalKeeper RAFT API(JK-RAFT API)
 * Journal Keeper Configuration API（JK-C API）
 * @author liyue25
 * Date: 2019-03-14
 * @param <Q> 状态查询条件类型
 * @param <R> 状态查询结果类型
 * @param <E> 日志的类型
 *
 */
public interface RaftClient<E, Q, R> extends Queryable<Q, R>, Watchable {


    /**
     * 写入操作日志变更状态。集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个update操作被执行。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     * @param entry 操作日志数组
     */
    CompletableFuture<Void> update(E entry);

    CompletableFuture<Void> update(E entry, int partition, int batchSize, ResponseConfig responseConfig);

    /**
     * 查询集群当前的状态，即日志在状态机中执行完成后产生的数据。该服务保证强一致性，保证读到的状态总是集群的最新状态。
     * @param query 查询条件
     * @return 查询结果
     */
    @Override
    CompletableFuture<R> query(Q query);

    /**
     * 获取集群配置。
     * @return 所有选民节点、观察者节点和当前的LEADER节点。
     */
    CompletableFuture<ClusterConfiguration> getServers();

    /**
     * 变更选民节点配置。
     * @param operation 操作，ADD：添加，REMOVE：删除
     * @param voter 需要添加或删除的节点地址。
     * @return true：成功，其它：失败。
     */
    CompletableFuture<Boolean> updateVoters(UpdateVoterOperation operation, URI voter);

    void stop();

    enum UpdateVoterOperation {ADD, REMOVE}
}
