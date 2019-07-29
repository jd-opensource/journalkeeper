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
package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.utils.event.EventBus;
import com.jd.journalkeeper.utils.event.EventWatcher;
import com.jd.journalkeeper.rpc.Detectable;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * Client调用Server的RPC
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClientServerRpc extends Detectable {

    /**
     * 获取当前连接Server的URI
     * @return 当前连接Server的URI
     */
    URI serverUri();

    /**
     * 客户端调用LEADER节点写入操作日志变更状态。
     * 集群保证按照提供的顺序写入，保证原子性，服务是线性的，任一时间只能有一个客户端使用该服务。
     * 日志在集群中复制到大多数节点，并在状态机执行后返回。
     *
     * @param request See {@link UpdateClusterStateRequest}
     * @return See {@link UpdateClusterStateResponse}
     */
    CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request);

    /**
     * 客户端调用LEADER节点查询集群当前的状态，即日志在状态机中执行完成后产生的数据。
     * 该服务保证强一致性，保证读到的状态总是集群的最新状态。
     *
     * @param request See {@link QueryStateRequest}
     * @return See {@link QueryStateResponse}
     */
    CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request);

    /**
     * 客户端调用任意节点查询节点当前的状态，即日志在状态机中执行完成后产生的数据。
     * 该服务不保证强一致性，只保证顺序一致，由于复制存在时延，集群中各节点的当前状态可能比集群的当前状态更旧。
     *
     * @param request See {@link QueryStateRequest}
     * @return See {@link QueryStateResponse}
     */
    CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request);

    /**
     * 客户端调用LEADER节点查询集群最新提交位置，用于二步读取。
     * @return See {@link LastAppliedResponse}
     */
    CompletableFuture<LastAppliedResponse> lastApplied();

    /**
     * 客户端查询任意节点上指定日志位置对应快照的状态，用于二步读取中，在非LEADER节点获取状态数据。
     *
     * @param request See {@link UpdateClusterStateRequest}
     * @return See {@link UpdateClusterStateResponse}
     * 可能的返回的状态：
     * StatusCode.INDEX_OVERFLOW: 请求位置对应的快照尚未生成。
     * StatusCode.INDEX_UNDERFLOW：请求位置对应的快照已删除。
     */
    CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request);

    /**
     * 客户端查询任意节点获取集群配置，返回集群所有节点和当前的LEADER节点。
     * 需要注意的是，只有LEADER节点上的配置是最新且准确的，在其它节点上查询到的集群配置有可能是已过期的旧配置。
     *
     * @return See {@link GetServersResponse}
     */
    CompletableFuture<GetServersResponse> getServers();
    /**
     * 添加pull模式事件监听。
     * @see EventBus
     * @return See {@link AddPullWatchResponse}
     */
    CompletableFuture<AddPullWatchResponse> addPullWatch();

    /**
     * 删除pull事件监听。
     * @see EventBus
     * @param request See {@link RemovePullWatchRequest}
     */
    CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request);

    /**
     * 拉取事件，并确认已拉取的事件位置。
     * @param request See {@link PullEventsRequest}
     * @see EventBus
     * @return See {@link PullEventsResponse}
     */
    CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request);

    /**
     * 添加事件监听器，当事件发生时会调用监听器
     * @see EventBus
     * @param eventWatcher 事件监听器
     */
    void watch(EventWatcher eventWatcher);
    /**
     * 删除事件监听器
     * @see EventBus
     * @param eventWatcher 事件监听器
     */
    void unWatch(EventWatcher eventWatcher);

    void stop();
}
