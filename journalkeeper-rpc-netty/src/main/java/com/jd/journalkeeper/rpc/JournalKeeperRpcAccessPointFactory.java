package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.client.JournalKeeperClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.codec.JournalKeeperCodec;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClientFactory;
import com.jd.journalkeeper.rpc.remoting.transport.TransportServer;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportClientFactory;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.rpc.server.ServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.handler.ServerRpcHandler;
import com.jd.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author liyue25
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
        return null;
    }

    @Override
    public ClientServerRpcAccessPoint createClientServerRpcAccessPoint(List<URI> servers, Properties properties) {
        ClientConfig clientConfig = toClientConfig(properties);
        return new JournalKeeperClientServerRpcAccessPoint(servers, transportClientFactory.create(clientConfig), properties);
    }

    @Override
    public StateServer bindServerService(ServerRpc serverRpc) {
        DefaultCommandHandlerFactory handlerFactory = new DefaultCommandHandlerFactory();
        handlerFactory.register(new ServerRpcHandler(serverRpc));
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
        // TODO
        return new ClientConfig();
    }


}
