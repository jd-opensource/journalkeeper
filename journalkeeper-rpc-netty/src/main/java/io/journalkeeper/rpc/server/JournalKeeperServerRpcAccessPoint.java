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
package io.journalkeeper.rpc.server;

import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.URIParser;
import io.journalkeeper.rpc.remoting.transport.TransportClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-03-30
 */
public class JournalKeeperServerRpcAccessPoint implements ServerRpcAccessPoint {
    private final Properties properties;
    private final TransportClient transportClient;
    private Map<URI, ServerRpcStub> serverInstances = new HashMap<>();
    private List<URIParser> uriParsers = new LinkedList<>();

    public JournalKeeperServerRpcAccessPoint(TransportClient transportClient, Properties properties) {
        this.transportClient = transportClient;
        try {
            transportClient.start();
        } catch (Exception e) {
            throw new RpcException(e);
        }
        this.properties = properties;
    }

    private ServerRpcStub createServerRpc(URI server) {
        return new ServerRpcStub(transportClient, server, parseUri(server));
    }
    @Override
    public void addUriParser(URIParser... uriParser) {
        uriParsers.addAll(Arrays.asList(uriParser));
    }
    @Override
    public ServerRpc getServerRpcAgent(URI uri) {
        if(null == uri ) return null;
        return serverInstances.computeIfAbsent(uri, this::createServerRpc);
    }

    @Override
    public void stop() {
        serverInstances.values().forEach(ServerRpcStub::stop);
        transportClient.stop();
    }

    private InetSocketAddress parseUri(URI uri) {

        for (URIParser uriParser : uriParsers) {
            for (String scheme : uriParser.supportedSchemes()) {
                if(scheme.equals(uri.getScheme())) {
                    return uriParser.parse(uri);
                }
            }
        }
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }
}
