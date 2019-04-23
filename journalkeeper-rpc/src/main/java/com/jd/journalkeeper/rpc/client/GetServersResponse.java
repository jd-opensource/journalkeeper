package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * RPC 方法
 * {@link ClientServerRpc#getServers() getServers()}
 * 返回响应。
 *
 * @author liyue25
 * Date: 2019-03-14
 */
public class GetServersResponse  extends BaseResponse {
    private final ClusterConfiguration clusterConfiguration;

    public GetServersResponse(Throwable exception) {
        this(exception, null);
    }

    public GetServersResponse(ClusterConfiguration clusterConfiguration) {
        this(null, clusterConfiguration);
    }
    private GetServersResponse(Throwable exception, ClusterConfiguration clusterConfiguration) {
        super(exception);
        this.clusterConfiguration = clusterConfiguration;
    }

    /**
     * 集群当前配置信息。
     * @return 集群当前配置信息。
     */
    public ClusterConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }
}
