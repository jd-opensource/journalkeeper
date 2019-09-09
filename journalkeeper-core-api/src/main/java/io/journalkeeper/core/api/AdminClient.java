package io.journalkeeper.core.api;

import io.journalkeeper.utils.event.Watchable;

import java.net.URI;
import java.util.List;
import java.util.Map;
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
     */
    CompletableFuture convertRoll(URI uri, RaftServer.Roll roll);

    /**
     * 压缩WAL。删除指定位置之前的WAL日志。
     * @param toIndices 每个分区的安全删除位置。
     */
    CompletableFuture compact(Map<Integer, Long> toIndices);

    /**
     * 变更集群分区配置
     * @param partitions 新分区配置
     */
    CompletableFuture scalePartitions(int[] partitions);

    /**
     * 获取节点当前状态
     * @param uri 节点uri
     * @return 节点当前状态. See {@link ServerStatus}
     */
    CompletableFuture<ServerStatus> getServerStatus(URI uri);

    void stop();

}
