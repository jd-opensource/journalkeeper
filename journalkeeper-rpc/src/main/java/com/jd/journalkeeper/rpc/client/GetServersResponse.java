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
