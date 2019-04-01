package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.rpc.client.ClientServerRpc;
import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.utils.state.StateServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class RpcTest {
    private ServerRpc serverRpcMock = mock(ServerRpc.class);
    ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private StateServer server;
    @Before
    public void before() throws IOException, URISyntaxException {
        int port = findRandomOpenPortOnAllLocalInterfaces();
        when(serverRpcMock.serverUri()).thenReturn(new URI("jk://localhost:" + port));

        RpcAccessPointFactory rpcAccessPointFactory = new JournalKeeperRpcAccessPointFactory();

        server = rpcAccessPointFactory.bindServerService(serverRpcMock);
        server.start();

        clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(Collections.singletonList(serverRpcMock.serverUri()),new Properties());
    }

    @Test
    public void testUpdateClusterState() throws ExecutionException, InterruptedException {
        int entrySize = 128;
        byte [] entry = new byte[entrySize];
        for (int i = 0; i < entrySize; i++) {
            entry[i] = (byte) i;
        }
        UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry);

        when(serverRpcMock.updateClusterState(any(UpdateClusterStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(UpdateClusterStateResponse::new));
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();

        UpdateClusterStateResponse response = clientServerRpc.updateClusterState(request).get();
        Assert.assertTrue(response.success());

        verify(serverRpcMock).updateClusterState(any(UpdateClusterStateRequest.class));
    }

    @After
    public void after() {
        if(null != server) server.stop();
    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

}
