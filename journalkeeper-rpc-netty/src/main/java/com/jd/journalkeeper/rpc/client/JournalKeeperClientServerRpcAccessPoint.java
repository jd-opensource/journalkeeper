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

import com.jd.journalkeeper.rpc.RpcException;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.utils.UriUtils;

import java.net.URI;
import java.util.*;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ClientServerRpcStub> serverInstances = new HashMap<>();
    private URI defaultServerUri = null;
    public JournalKeeperClientServerRpcAccessPoint(List<URI> servers, TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            this.transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        if(null != servers) {
            servers.forEach(server -> serverInstances.put(server, null));
        }
        this.properties = properties;
    }

    @Override
    public void updateServers(List<URI> uriList) {
        // 删除
        serverInstances.keySet().stream()
                .filter(uri -> !uriList.contains(uri))
                .map(serverInstances::remove)
                .forEach(this::disconnect);
        // 增加
        uriList.forEach(uri -> serverInstances.putIfAbsent(uri, null));


    }

    @Override
    public ClientServerRpc defaultClientServerRpc() {
        return getClintServerRpc(selectServer());
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if(null == uri ) return null;
        return serverInstances.computeIfAbsent(uri, this::connect);
    }

    @Override
    public void stop() {
        serverInstances.values()
                .forEach(this::disconnect);
        transportClient.stop();
    }

    private URI selectServer() {
        if(null == defaultServerUri) {
            defaultServerUri = serverInstances.entrySet().stream()
                    .filter(entry -> Objects.nonNull(entry.getValue()))
                    .filter(entry -> entry.getValue().isAlive())
                    .map(Map.Entry::getKey).findAny().
                            orElse(serverInstances.keySet().stream().findAny().orElse(null));
        }

        return defaultServerUri;
    }

    private ClientServerRpcStub connect(URI server) {
        Transport transport = transportClient.createTransport(UriUtils.toSockAddress(server));
        return new ClientServerRpcStub(transport, server);
    }

    private void disconnect(ClientServerRpcStub clientServerRpc) {
        if(null != clientServerRpc) {
            clientServerRpc.stop();
        }
    }


}
