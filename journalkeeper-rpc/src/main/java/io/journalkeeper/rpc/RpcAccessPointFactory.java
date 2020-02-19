/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc;

import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.state.StateServer;

import java.util.Properties;

/**
 * RPC实现的入口工厂类
 * @author LiYue
 * Date: 2019-03-26
 */
public interface RpcAccessPointFactory {
    /**
     * 创建一个ServerRpcAccessPoint接入点
     * @param properties 属性值
     * @return ServerRpcAccessPoint实例
     */
    ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties);

    /**
     * 创建一个ClientServerRpcAccessPoint接入点
     * @param properties 属性值
     * @return ClientServerRpcAccessPoint实例
     */
    ClientServerRpcAccessPoint createClientServerRpcAccessPoint(Properties properties);

    /**
     * 将serverRpc绑定到服务上，绑定后serverRpc可以对外提供RPC服务。
     * @param serverRpc 提供服务的serverRpc
     * @return StateServer实例，用于安全关闭已绑定的服务。
     */
    StateServer bindServerService(ServerRpc serverRpc);

}
