package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.exceptions.IndexOverflowException;
import com.jd.journalkeeper.exceptions.IndexUnderflowException;
import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.rpc.client.*;
import com.jd.journalkeeper.rpc.server.ServerRpc;
import com.jd.journalkeeper.utils.state.StateServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class RpcTest {
    private static final Logger logger = LoggerFactory.getLogger(RpcTest.class);
    private ServerRpc serverRpcMock = mock(ServerRpc.class);
    private ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private StateServer server;
    @Before
    public void before() throws IOException, URISyntaxException {
        int port = findRandomOpenPortOnAllLocalInterfaces();
        ;
        when(serverRpcMock.serverUri()).thenReturn(new URI("jk://localhost:" + port));
        logger.info("Server URI: {}", serverRpcMock.serverUri());
        RpcAccessPointFactory rpcAccessPointFactory = new JournalKeeperRpcAccessPointFactory();

        server = rpcAccessPointFactory.bindServerService(serverRpcMock);
        server.start();

        clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(Collections.singletonList(serverRpcMock.serverUri()),new Properties());
    }

    @Test
    public  void testException() throws ExecutionException, InterruptedException {
        // Test exception response
        LastAppliedResponse response;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();

        String errorMsg = "原谅他们是上帝的事，我们的任务是负责送他们见上帝。 --普京";
        Throwable t = new RuntimeException(errorMsg);
        when(serverRpcMock.lastApplied()).thenThrow(t);
        response = clientServerRpc.lastApplied().get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.EXCEPTION, response.getStatusCode());
        Assert.assertTrue(response.getError().contains(errorMsg));
    }

    @Test
    public  void testResponseException() throws ExecutionException, InterruptedException {
        // Test exception response
        LastAppliedResponse response;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();

        String errorMsg = "原谅他们是上帝的事，我们的任务是负责送他们见上帝。 --普京";
        Throwable t = new RuntimeException(errorMsg);
        when(serverRpcMock.lastApplied())
                .thenReturn(CompletableFuture.supplyAsync(() -> new LastAppliedResponse(t)));
        response = clientServerRpc.lastApplied().get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.EXCEPTION, response.getStatusCode());
        Assert.assertTrue(response.getError().contains(errorMsg));
    }
    @Test
    public void testNotLeader() throws ExecutionException, InterruptedException {
        LastAppliedResponse response;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();

        String leaderUriStr = "jk://leader.host:8888";
        when(serverRpcMock.lastApplied())
                .thenThrow(new NotLeaderException(URI.create(leaderUriStr)));
        response = clientServerRpc.lastApplied().get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.NOT_LEADER, response.getStatusCode());
        Assert.assertEquals(leaderUriStr, response.getLeader().toString());
    }

    @Test
    public void testUpdateClusterState() throws ExecutionException, InterruptedException {
        int entrySize = 128;
        byte [] entry = new byte[entrySize];
        for (int i = 0; i < entrySize; i++) {
            entry[i] = (byte) i;
        }
        UpdateClusterStateRequest request = new UpdateClusterStateRequest(entry);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();
        UpdateClusterStateResponse response;
        // Test success response
        when(serverRpcMock.updateClusterState(any(UpdateClusterStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(UpdateClusterStateResponse::new));
        response = clientServerRpc.updateClusterState(request).get();
        Assert.assertTrue(response.success());
        verify(serverRpcMock).updateClusterState(argThat((UpdateClusterStateRequest r) -> Arrays.equals(entry, r.getEntry())));

    }

    @Test
    public void testQueryClusterState() throws ExecutionException, InterruptedException {
        int querySize = 128;
        int resultSize = 55;
        byte [] query = new byte[querySize];
        for (int i = 0; i < querySize; i++) {
            query[i] = (byte) i;
        }
        byte [] result = new byte[resultSize];
        for (int i = 0; i < resultSize; i++) {
            result[i] = (byte) i;
        }
        QueryStateRequest request = new QueryStateRequest(query);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();
        QueryStateResponse response;
        // Test success response
        when(serverRpcMock.queryClusterState(any(QueryStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new QueryStateResponse(result)));
        response = clientServerRpc.queryClusterState(request).get();
        Assert.assertTrue(response.success());
        Assert.assertArrayEquals(result, response.getResult());

        verify(serverRpcMock).queryClusterState(
                argThat((QueryStateRequest r) -> Arrays.equals(query, r.getQuery())));

    }


    @Test
    public void testQueryServerState() throws ExecutionException, InterruptedException {
        int querySize = 128;
        int resultSize = 55;
        byte [] query = new byte[querySize];
        for (int i = 0; i < querySize; i++) {
            query[i] = (byte) i;
        }
        byte [] result = new byte[resultSize];
        for (int i = 0; i < resultSize; i++) {
            result[i] = (byte) i;
        }
        long lastApplied = -993L;
        QueryStateRequest request = new QueryStateRequest(query);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();
        QueryStateResponse response;
        // Test success response
        when(serverRpcMock.queryServerState(any(QueryStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new QueryStateResponse(result, lastApplied)));
        response = clientServerRpc.queryServerState(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(lastApplied, response.getLastApplied());
        Assert.assertArrayEquals(result, response.getResult());

        verify(serverRpcMock).queryServerState(
                argThat((QueryStateRequest r) -> Arrays.equals(query, r.getQuery())));

    }

    @Test
    public void testQuerySnapshotState() throws ExecutionException, InterruptedException {
        int querySize = 128;
        int resultSize = 55;
        byte [] query = new byte[querySize];
        for (int i = 0; i < querySize; i++) {
            query[i] = (byte) i;
        }
        byte [] result = new byte[resultSize];
        for (int i = 0; i < resultSize; i++) {
            result[i] = (byte) i;
        }
        long index = 23339L;
        QueryStateRequest request = new QueryStateRequest(query, index);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();
        QueryStateResponse response;
        // Test success response
        when(serverRpcMock.queryServerState(any(QueryStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new QueryStateResponse(result)));
        response = clientServerRpc.queryServerState(request).get();
        Assert.assertTrue(response.success());
        Assert.assertArrayEquals(result, response.getResult());

        verify(serverRpcMock).queryServerState(
                argThat((QueryStateRequest r) -> Arrays.equals(query, r.getQuery()) && r.getIndex() == index));


        // Test index overflow
        when(serverRpcMock.queryServerState(any(QueryStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new QueryStateResponse(new IndexOverflowException())));
        response = clientServerRpc.queryServerState(request).get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.INDEX_OVERFLOW, response.getStatusCode());

        // Test index underflow
        when(serverRpcMock.queryServerState(any(QueryStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new QueryStateResponse(new IndexUnderflowException())));
        response = clientServerRpc.queryServerState(request).get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.INDEX_UNDERFLOW, response.getStatusCode());


    }

    @Test
    public void testGerServers() throws ExecutionException, InterruptedException {
        URI leader = URI.create("jk://leader_host:8888");
        List<URI> observers = null;
        List<URI> voters = Arrays.asList(
                URI.create("jk://voter1_host:8888"),
                URI.create("jk://leader_host:8888"),
                URI.create("jk://voter2_host:8888"));
        ClusterConfiguration clusterConfiguration = new ClusterConfiguration(leader, voters, observers);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc();
        GetServersResponse response;

        when(serverRpcMock.getServers())
                .thenReturn(CompletableFuture.supplyAsync(() -> new GetServersResponse(clusterConfiguration)));
        response = clientServerRpc.getServers().get();
        Assert.assertTrue(response.success());

        Assert.assertEquals(leader, response.getClusterConfiguration().getLeader());
        Assert.assertEquals(voters, response.getClusterConfiguration().getVoters());
        Assert.assertTrue(response.getClusterConfiguration().getObservers().isEmpty());
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
