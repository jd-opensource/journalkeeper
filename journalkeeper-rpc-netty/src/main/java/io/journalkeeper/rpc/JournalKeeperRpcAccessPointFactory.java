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
import io.journalkeeper.rpc.client.JournalKeeperClientServerRpcAccessPoint;
import io.journalkeeper.rpc.codec.JournalKeeperCodec;
import io.journalkeeper.rpc.handler.ServerRpcCommandHandlerRegistry;
import io.journalkeeper.rpc.remoting.transport.TransportClientFactory;
import io.journalkeeper.rpc.remoting.transport.TransportServer;
import io.journalkeeper.rpc.remoting.transport.command.support.UriRoutedCommandHandlerFactory;
import io.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import io.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import io.journalkeeper.rpc.remoting.transport.support.DefaultTransportClientFactory;
import io.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;
import io.journalkeeper.rpc.server.JournalKeeperServerRpcAccessPoint;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.spi.Singleton;
import io.journalkeeper.utils.state.ServerStateMachine;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author LiYue
 * Date: 2019-03-30
 */
@Singleton
public class JournalKeeperRpcAccessPointFactory implements RpcAccessPointFactory {
    private final Logger LOG= LoggerFactory.getLogger(JournalKeeperRpcAccessPointFactory.class);
    private final TransportClientFactory transportClientFactory;
    private final Map<InetSocketAddress /* server port */, TransportServerAndReferenceCount> transportServerMap =
            new HashMap<>();
    private final DefaultTransportServerFactory defaultTransportServerFactory;
    private final UriRoutedCommandHandlerFactory handlerFactory;

    public JournalKeeperRpcAccessPointFactory() {
        JournalKeeperCodec journalKeeperCodec = new JournalKeeperCodec();
        transportClientFactory = new DefaultTransportClientFactory(journalKeeperCodec);
        handlerFactory = new UriRoutedCommandHandlerFactory();

        defaultTransportServerFactory = new DefaultTransportServerFactory(
                new JournalKeeperCodec(), handlerFactory
        );
    }

    @Override
    public ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties) {
        ClientConfig clientConfig = toClientConfig(properties);
        return new JournalKeeperServerRpcAccessPoint(transportClientFactory.create(clientConfig), properties);
    }

    @Override
    public ClientServerRpcAccessPoint createClientServerRpcAccessPoint(Properties properties) {
        ClientConfig clientConfig = toClientConfig(properties);
        return new JournalKeeperClientServerRpcAccessPoint(transportClientFactory.create(clientConfig), properties);
    }

    @Override
    public synchronized StateServer bindServerService(ServerRpc serverRpc) {

        InetSocketAddress address = UriSupport.parseUri(serverRpc.serverUri());
        TransportServerAndReferenceCount server = transportServerMap.computeIfAbsent(address, addr -> {
            try {
                TransportServer ts = defaultTransportServerFactory
                        .bind(new ServerConfig(), addr.getHostName(), addr.getPort());
                ts.start();
                LOG.info("Bind server on {}:{} ",addr.getHostName(),addr.getPort());
                return new TransportServerAndReferenceCount(ts);
            } catch (Throwable t) {
                throw new RpcException(t);
            }
        });
        server.getReferenceCounter().incrementAndGet();
        ServerRpcCommandHandlerRegistry.register(handlerFactory, serverRpc);

        return new ServerStateMachine(true) {
            @Override
            protected void doStop() {
                super.doStop();
                int ref = server.getReferenceCounter().decrementAndGet();
                handlerFactory.unRegister(serverRpc.serverUri());
                if (ref <= 0) {
                    synchronized (JournalKeeperRpcAccessPointFactory.this) {

                        transportServerMap.remove(address);
                        server.getTransportServer().stop();

                    }
                }
            }
        };

    }

    private ClientConfig toClientConfig(Properties properties) {
        return new ClientConfig();
    }


    private static class TransportServerAndReferenceCount {
        private final TransportServer transportServer;
        private final AtomicInteger referenceCounter = new AtomicInteger(0);

        public TransportServerAndReferenceCount(TransportServer transportServer) {
            this.transportServer = transportServer;
        }

        public TransportServer getTransportServer() {
            return transportServer;
        }

        public AtomicInteger getReferenceCounter() {
            return referenceCounter;
        }
    }
}
