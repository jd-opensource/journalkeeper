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
package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.utils.UriUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ClientServerRpcStub> serverInstances = new ConcurrentHashMap<>();
    private List<URI> servers = new ArrayList<>();
    private ClientServerRpc defaultRpc = null;
    public JournalKeeperClientServerRpcAccessPoint(Collection<URI> servers, TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            this.transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        if(null != servers) {
            this.servers.addAll(servers);
        }
        this.properties = properties;
    }

    @Override
    public synchronized void updateServers(Collection<URI> uriList) {
        // 删除
        serverInstances.keySet().stream()
                .filter(uri -> !uriList.contains(uri))
                .map(serverInstances::remove)
                .forEach(this::disconnect);
        this.servers.removeIf(uri -> !uriList.contains(uri));
        // 增加
        this.servers.addAll(uriList.stream().filter(uri -> !servers.contains(uri)).collect(Collectors.toList()));

    }

    @Override
    public ClientServerRpc defaultClientServerRpc() {
        return null != defaultRpc && defaultRpc.isAlive() ? defaultRpc: getClintServerRpc(selectServer());
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if(null == uri ) return null;
        ClientServerRpcStub clientServerRpc = serverInstances.get(uri);
        if(null != clientServerRpc && clientServerRpc.isAlive()) {
            return clientServerRpc;
        } else {
            if(null != clientServerRpc) {
                serverInstances.remove(uri);
                stopQuiet(clientServerRpc);
            }
            clientServerRpc = connect(uri);
            serverInstances.put(uri, clientServerRpc);
            return clientServerRpc;
        }
    }

    private void stopQuiet(ClientServerRpc clientServerRpc) {
        try {
            if (null != clientServerRpc) {
                clientServerRpc.stop();
            }
        } catch (Throwable ignored) {}// ignore stop exception of dead rpc instance
    }

    @Override
    public void stop() {
        serverInstances.values()
                .forEach(this::disconnect);
        transportClient.stop();
    }

    private URI selectServer() {
        return servers.get(ThreadLocalRandom.current().nextInt(servers.size()));
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
