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
package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.server.ServerRpcAccessPoint;
import com.jd.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * RPC实现的入口工厂类
 * @author liyue25
 * Date: 2019-03-26
 */
public interface RpcAccessPointFactory {
    /**
     * 创建一个ServerRpc接入点
     */
    ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties);

    /**
     * 创建一个ClientServerRpc接入点
     * @param servers 可连接的服务器列表
     */
    ClientServerRpcAccessPoint createClientServerRpcAccessPoint(List<URI> servers, Properties properties);

    StateServer bindServerService(ServerRpc serverRpc);

}
