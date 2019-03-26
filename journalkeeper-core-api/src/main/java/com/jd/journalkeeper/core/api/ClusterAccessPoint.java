package com.jd.journalkeeper.core.api;

/**
 * 集群访问接入点
 * @author liyue25
 * Date: 2019-03-14
 */
public interface ClusterAccessPoint<E, Q, R> {
    /**
     * 获取客户端实例
     */
    JournalKeeperClient<E, Q, R> getClient();

    /**
     * 获取Server实例，如果本地存在Server返回Server实例，否则返回null
     */
    JournalKeeperServer<E, Q, R> getServer();
}
