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

import io.journalkeeper.utils.event.Watchable;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * 用于集群管理的接口
 * @author LiYue
 * Date: 2019-09-09
 */
public interface AdminClient extends Watchable, ClusterReadyAware, ServerConfigAware {

    /**
     * 获取集群配置。
     * @return 所有选民节点、观察者节点和当前的LEADER节点。
     */
    CompletableFuture<ClusterConfiguration> getClusterConfiguration();

    /**
     * 获取指定节点上的集群配置。
     * @param uri 节点uri
     * @return 所有选民节点、观察者节点和当前的LEADER节点。
     */
    CompletableFuture<ClusterConfiguration> getClusterConfiguration(URI uri);

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
     * @return 执行成功返回null，失败抛出异常。
     */
    CompletableFuture<Void> convertRoll(URI uri, RaftServer.Roll roll);

    /**
     * 变更集群分区配置
     * @param partitions 新分区配置
     * @return 执行成功返回null，失败抛出异常。
     */
    CompletableFuture<Void> scalePartitions(Set<Integer> partitions);

    /**
     * 获取节点当前状态
     * @param uri 节点uri
     * @return 节点当前状态. See {@link ServerStatus}
     */
    CompletableFuture<ServerStatus> getServerStatus(URI uri);

    /**
     * 设置集群推荐Leader。当集群状态正常并且推荐的Leader节点上的日志
     * 与当前Leader节点日志保持同步后，
     * 集群会自动将Leader节点切换为推荐的Leader节点。
     *
     * @param preferredLeader 推荐的Leader
     * @return 执行成功返回null，失败抛出异常。
     */
    CompletableFuture<Void> setPreferredLeader(URI preferredLeader);

    /**
     * 创建一个快照
     *
     * @return 快照位置
     */
    CompletableFuture<Void> takeSnapshot();

    /**
     *
     * 恢复快照
     * @param index 快照的索引位置
     * @return
     */
    CompletableFuture<Void> recoverSnapshot(long index);

    void stop();

}
