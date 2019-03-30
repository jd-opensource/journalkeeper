package com.jd.journalkeeper.rpc.client;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperClientServerRpcAccessPoint implements ClientServerRpcAccessPoint {
    private final Properties properties;
    private ClientServerRpc defaultStub = null;
    public JournalKeeperClientServerRpcAccessPoint(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void updateServers(List<URI> uriList) {
    }

    @Override
    public ClientServerRpc getClintServerRpc() {
        return null;
    }

    @Override
    public ClientServerRpc getClintServerRpc(URI uri) {
        return null;
    }

    @Override
    public void setServiceProvider(ClientServerRpc clientServerRpc) {

    }
}
