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
package com.jd.journalkeeper.core;

import com.jd.journalkeeper.core.server.Server;
import com.jd.journalkeeper.rpc.client.ClientServerRpc;
import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;

import java.net.URI;
import java.util.List;

/**
 * 优先访问本地的ClientServerRpc接入点
 * @author liyue25
 * Date: 2019-03-25
 */
public class LocalDefaultRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Server server;
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    public LocalDefaultRpcAccessPoint(Server server, ClientServerRpcAccessPoint clientServerRpcAccessPoint) {
        this.server = server;
        this.clientServerRpcAccessPoint = clientServerRpcAccessPoint;
    }

    @Override
    public void updateServers(List<URI> uriList) {
        // nothing to do
    }

    @Override
    public ClientServerRpc defaultClientServerRpc() {
        return server;
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        return uri.equals(server.serverUri()) ?  server: clientServerRpcAccessPoint.getClintServerRpc(uri);
    }

    @Override
    public void stop() {
        clientServerRpcAccessPoint.stop();
    }

}
