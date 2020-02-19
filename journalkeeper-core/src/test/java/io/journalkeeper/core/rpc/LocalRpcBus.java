package io.journalkeeper.core.rpc;

import io.journalkeeper.exceptions.ServerNotFoundException;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.spi.Singleton;
import io.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
@Singleton
public class LocalRpcBus implements ClientServerRpcAccessPoint, ServerRpcAccessPoint, RpcAccessPointFactory {

    private final Map<URI, ServerRpc> providers = new ConcurrentHashMap<>();

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        ClientServerRpc clientServerRpc = providers.get(uri);
        if (null == clientServerRpc) {
            throw new ServerNotFoundException("No provider for uri: " + uri + "!");
        }
        return clientServerRpc;
    }

    @Override
    public void stop() {

    }

    @Override
    public ServerRpc getServerRpcAgent(URI uri) {
        return providers.get(uri);
    }

    @Override
    public ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties) {
        return this;
    }

    @Override
    public ClientServerRpcAccessPoint createClientServerRpcAccessPoint(Properties properties) {
        return this;
    }

    @Override
    public StateServer bindServerService(ServerRpc serverRpc) {
        providers.put(serverRpc.serverUri(), new WrappedServerRpc(serverRpc));
        return new StateServer() {
            private ServerState state = ServerState.CREATED;

            @Override
            public void start() {
                state = ServerState.RUNNING;
            }

            @Override
            public void stop() {
                providers.remove(serverRpc.serverUri());
                state = ServerState.STOPPED;
            }

            @Override
            public ServerState serverState() {
                return state;
            }
        };
    }
}
