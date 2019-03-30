package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.client.JournalKeeperClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.server.ServerRpcAccessPoint;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-03-30
 */
public class JournalKeeperRpcAccessPointFactory implements RpcAccessPointFactory {
    @Override
    public ServerRpcAccessPoint createServerRpcAccessPoint(Properties properties) {
        return null;
    }

    @Override
    public ClientServerRpcAccessPoint createClientServerRpcAccessPoint(List<URI> servers, Properties properties) {
        return new JournalKeeperClientServerRpcAccessPoint(properties);
    }
}
