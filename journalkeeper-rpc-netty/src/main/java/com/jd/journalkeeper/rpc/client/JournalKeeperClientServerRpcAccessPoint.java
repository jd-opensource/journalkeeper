package com.jd.journalkeeper.rpc.client;

import java.net.URI;
import java.util.*;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private Map<URI, ClientServerRpc> serverInstances = new HashMap<>();
    private URI currentServerUri = null;
    public JournalKeeperClientServerRpcAccessPoint(List<URI> servers, Properties properties) {
        servers.forEach(server -> serverInstances.put(server, null));
        this.properties = properties;
    }

    @Override
    public void updateServers(List<URI> uriList) {
    }

    @Override
    public ClientServerRpc getClintServerRpc() {
        return getClintServerRpc(selectServer());
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        if(null == uri ) return null;
        return serverInstances.computeIfAbsent(uri, this::connect);
    }

    @Override
    public void setServiceProvider(ClientServerRpc clientServerRpc) {

    }

    private URI selectServer() {
        if(null == currentServerUri) {
            currentServerUri = serverInstances.entrySet().stream()
                    .filter(entry -> Objects.nonNull(entry.getValue()))
                    .filter(entry -> entry.getValue().isAlive())
                    .map(Map.Entry::getKey).findAny().
                            orElse(serverInstances.keySet().stream().findAny().orElse(null));
        }

        return currentServerUri;
    }

    private ClientServerRpc connect(URI server) {
        // TODO
        return null;
    }
}
