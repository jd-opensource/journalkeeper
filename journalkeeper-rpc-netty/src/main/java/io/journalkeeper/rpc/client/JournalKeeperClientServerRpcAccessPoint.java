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
import io.journalkeeper.rpc.URIParser;
import io.journalkeeper.rpc.UriSupport;
import io.journalkeeper.rpc.remoting.transport.TransportClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author LiYue
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ClientServerRpcStub> serverInstances = new ConcurrentHashMap<>();
    public JournalKeeperClientServerRpcAccessPoint(TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            this.transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        this.properties = properties;
    }


    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if(null == uri ){
            throw new IllegalArgumentException("URI can not be null!");
        }
        return serverInstances.computeIfAbsent(uri, this::createClientServerRpc);
    }

    @Override
    public void stop() {
        serverInstances.values()
                .forEach(this::disconnect);
        transportClient.stop();
    }

    private ClientServerRpcStub createClientServerRpc(URI server) {
        return new ClientServerRpcStub(transportClient, server, UriSupport.parseUri(server));
    }

    private void disconnect(ClientServerRpcStub clientServerRpc) {
        if(null != clientServerRpc) {
            clientServerRpc.stop();
        }
    }


}
