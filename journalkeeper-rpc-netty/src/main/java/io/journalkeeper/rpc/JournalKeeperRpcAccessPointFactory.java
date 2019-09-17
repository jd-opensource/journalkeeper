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
package io.journalkeeper.rpc;

import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.JournalKeeperClientServerRpcAccessPoint;
import io.journalkeeper.rpc.codec.JournalKeeperCodec;
import io.journalkeeper.rpc.handler.ServerRpcCommandHandlerRegistry;
import io.journalkeeper.rpc.remoting.transport.TransportClientFactory;
import io.journalkeeper.rpc.remoting.transport.TransportServer;
import io.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import io.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import io.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import io.journalkeeper.rpc.remoting.transport.support.DefaultTransportClientFactory;
import io.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;
import io.journalkeeper.rpc.server.JournalKeeperServerRpcAccessPoint;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-03-30
 */
public class JournalKeeperRpcAccessPointFactory implements RpcAccessPointFactory {
    private final TransportClientFactory transportClientFactory;

    public JournalKeeperRpcAccessPointFactory() {
        JournalKeeperCodec journalKeeperCodec = new JournalKeeperCodec();
        transportClientFactory = new DefaultTransportClientFactory(journalKeeperCodec);
    }
    @Override
    public ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties) {
        ClientConfig clientConfig = toClientConfig(properties);
        return new JournalKeeperServerRpcAccessPoint(transportClientFactory.create(clientConfig), properties);
    }

    @Override
    public ClientServerRpcAccessPoint createClientServerRpcAccessPoint( Properties properties) {
        ClientConfig clientConfig = toClientConfig(properties);
        return new JournalKeeperClientServerRpcAccessPoint(transportClientFactory.create(clientConfig), properties);
    }

    @Override
    public StateServer bindServerService(ServerRpc serverRpc) {
        DefaultCommandHandlerFactory handlerFactory = new DefaultCommandHandlerFactory();
        ServerRpcCommandHandlerRegistry.register(handlerFactory, serverRpc);
        DefaultTransportServerFactory defaultTransportServerFactory = new DefaultTransportServerFactory(
                new JournalKeeperCodec(), handlerFactory
        );
        TransportServer server = defaultTransportServerFactory
                .bind(new ServerConfig(), serverRpc.serverUri().getHost(), serverRpc.serverUri().getPort());
        return new StateServer() {
            @Override
            public void start() {
                try {
                    server.start();
                } catch (Exception e) {
                    throw new RpcException(e);
                }
            }

            @Override
            public void stop() {
                server.stop();
            }

            @Override
            public ServerState serverState() {
                return server.isStarted()? ServerState.RUNNING: ServerState.STOPPED;
            }
        };
    }

    private ClientConfig toClientConfig(Properties properties) {
        // TODO ClientConfig可配置
        return new ClientConfig();
    }


}
