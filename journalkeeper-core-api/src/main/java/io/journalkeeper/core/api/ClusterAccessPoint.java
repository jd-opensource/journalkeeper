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

/**
 * 集群访问接入点
 * @author LiYue
 * Date: 2019-03-14
 */
public interface ClusterAccessPoint<E, ER, Q, QR> {
    /**
     * 获取客户端实例
     */
    RaftClient<E, ER, Q, QR> getClient();

    /**
     * 获取Server实例，如果本地存在Server返回Server实例，否则返回null
     */
    RaftServer<E, ER, Q, QR> getServer();

    /**
     * 获取管理端实例
     */
    AdminClient getAdminClient();
}
