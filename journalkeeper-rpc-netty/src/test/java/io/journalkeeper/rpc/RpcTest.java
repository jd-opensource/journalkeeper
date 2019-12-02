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

import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.SerializedUpdateRequest;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.exceptions.IndexOverflowException;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.rpc.client.AddPullWatchResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.rpc.client.CompleteTransactionRequest;
import io.journalkeeper.rpc.client.CompleteTransactionResponse;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.client.ConvertRollResponse;
import io.journalkeeper.rpc.client.CreateTransactionRequest;
import io.journalkeeper.rpc.client.CreateTransactionResponse;
import io.journalkeeper.rpc.client.GetOpeningTransactionsResponse;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.client.PullEventsRequest;
import io.journalkeeper.rpc.client.PullEventsResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.RemovePullWatchRequest;
import io.journalkeeper.rpc.client.RemovePullWatchResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.rpc.client.UpdateVotersResponse;
import io.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.journalkeeper.rpc.server.DisableLeaderWriteRequest;
import io.journalkeeper.rpc.server.DisableLeaderWriteResponse;
import io.journalkeeper.rpc.server.GetServerEntriesRequest;
import io.journalkeeper.rpc.server.GetServerEntriesResponse;
import io.journalkeeper.rpc.server.GetServerStateRequest;
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.journalkeeper.rpc.server.RequestVoteRequest;
import io.journalkeeper.rpc.server.RequestVoteResponse;
import io.journalkeeper.rpc.server.ServerRpc;
import io.journalkeeper.rpc.server.ServerRpcAccessPoint;
import io.journalkeeper.utils.event.Event;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.PullEvent;
import io.journalkeeper.utils.net.NetworkingUtils;
import io.journalkeeper.utils.state.StateServer;
import io.journalkeeper.utils.test.ByteUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author LiYue
 * Date: 2019-04-01
 */
public class RpcTest {
    private static final Logger logger = LoggerFactory.getLogger(RpcTest.class);
    private ServerRpc serverRpcMock = mock(ServerRpc.class);
    private ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private ServerRpcAccessPoint serverRpcAccessPoint;
    private StateServer server;
    @Before
    public void before() throws IOException, URISyntaxException {
        int port = NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces();
        when(serverRpcMock.serverUri()).thenReturn(new URI("jk://localhost:" + port));
        logger.info("Server URI: {}", serverRpcMock.serverUri());
        RpcAccessPointFactory rpcAccessPointFactory = new JournalKeeperRpcAccessPointFactory();

        server = rpcAccessPointFactory.bindServerService(serverRpcMock);
        server.start();

        clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(new Properties());
        serverRpcAccessPoint = rpcAccessPointFactory.createServerRpcAccessPoint(new Properties());
    }

    @Test
    public  void testException() throws ExecutionException, InterruptedException {
        // Test exception response
        LastAppliedResponse response;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());

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
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());

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
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());

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
        List<SerializedUpdateRequest> entries = Arrays.asList(
              new SerializedUpdateRequest(ByteUtils.createRandomSizeBytes(128), 0, 1),
              new SerializedUpdateRequest(ByteUtils.createRandomSizeBytes(128), 0, 1),
              new SerializedUpdateRequest(ByteUtils.createRandomSizeBytes(128), 0, 1)
        );
        UpdateClusterStateRequest request = new UpdateClusterStateRequest(UUID.randomUUID(), entries, false, ResponseConfig.RECEIVE);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        UpdateClusterStateResponse response, serverResponse;
        serverResponse = new UpdateClusterStateResponse(
          ByteUtils.createFixedSizeByteList(32, 3)
        );
        // Test success response
        when(serverRpcMock.updateClusterState(any(UpdateClusterStateRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(serverResponse));
        response = clientServerRpc.updateClusterState(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(response.getResults().size(), serverResponse.getResults().size());
        for (int i = 0; i < response.getResults().size(); i++) {
            Assert.assertArrayEquals(response.getResults().get(i), serverResponse.getResults().get(i));
        }

        verify(serverRpcMock).updateClusterState(argThat((UpdateClusterStateRequest r) -> {
                    if (request.getTransactionId().equals(r.getTransactionId()) &&
                            request.getRequests().size() == r.getRequests().size() &&
                            request.isIncludeHeader() == r.isIncludeHeader() &&
                            request.getResponseConfig() == r.getResponseConfig()) {
                        for (int i = 0; i < request.getRequests().size(); i++) {
                            Assert.assertArrayEquals(request.getRequests().get(i).getEntry(), r.getRequests().get(i).getEntry());
                            Assert.assertEquals(request.getRequests().get(i).getPartition(), r.getRequests().get(i).getPartition());
                            Assert.assertEquals(request.getRequests().get(i).getBatchSize(), r.getRequests().get(i).getBatchSize());
                        }
                        return true;
                    }
                    return false;
                }

        ));

    }


    @Test
    public void testUpdateVoters() throws ExecutionException, InterruptedException {
        final List<URI> oldConfig = Arrays.asList(
                URI.create("jk://192.168.0.1:8888"),
                URI.create("jk://192.168.0.1:8889"),
                URI.create("jk://192.168.0.1:8890")
                );

        final List<URI> newConfig = Arrays.asList(
                URI.create("jk://192.168.0.1:8888"),
                URI.create("jk://192.168.0.1:8889"),
                URI.create("jk://192.168.0.1:8891")
                );

        UpdateVotersRequest request = new UpdateVotersRequest(oldConfig, newConfig);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        UpdateVotersResponse response;
        // Test success response
        when(serverRpcMock.updateVoters(any(UpdateVotersRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(UpdateVotersResponse::new));
        response = clientServerRpc.updateVoters(request).get();
        Assert.assertTrue(response.success());
        verify(serverRpcMock).updateVoters(argThat((UpdateVotersRequest r) ->
                oldConfig.equals(r.getOldConfig()) &&
                        newConfig.equals(r.getNewConfig())
                ));

    }

    @Test
    public void testConvertRoll() throws ExecutionException, InterruptedException {
        RaftServer.Roll roll = RaftServer.Roll.VOTER;

        ConvertRollRequest request = new ConvertRollRequest(roll);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        ConvertRollResponse response;
        // Test success response
        when(serverRpcMock.convertRoll(any(ConvertRollRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(ConvertRollResponse::new));
        response = clientServerRpc.convertRoll(request).get();
        Assert.assertTrue(response.success());
        verify(serverRpcMock).convertRoll(argThat((ConvertRollRequest r) ->
                roll == r.getRoll()
                ));

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
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
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
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
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
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
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
                URI.create("jk://192.168.8.8:8888"));
        ClusterConfiguration clusterConfiguration = new ClusterConfiguration(leader, voters, observers);
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        GetServersResponse response;

        when(serverRpcMock.getServers())
                .thenReturn(CompletableFuture.supplyAsync(() -> new GetServersResponse(clusterConfiguration)));
        response = clientServerRpc.getServers().get();
        Assert.assertTrue(response.success());

        Assert.assertEquals(leader, response.getClusterConfiguration().getLeader());
        Assert.assertEquals(voters, response.getClusterConfiguration().getVoters());
        Assert.assertNull(response.getClusterConfiguration().getObservers());
    }


    @Test
    public void testAddPullWatch() throws ExecutionException, InterruptedException {

        long pullWatchId = 666L;
        long pullIntervalMs =  10000L;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        AddPullWatchResponse response;

        when(serverRpcMock.addPullWatch())
                .thenReturn(CompletableFuture.supplyAsync(() -> new AddPullWatchResponse(pullWatchId, pullIntervalMs)));
        response = clientServerRpc.addPullWatch().get();
        Assert.assertTrue(response.success());

        Assert.assertEquals(pullWatchId, response.getPullWatchId());
        Assert.assertEquals(pullIntervalMs, response.getPullIntervalMs());
    }


    @Test
    public void testRemovePullWatch() throws ExecutionException, InterruptedException {

        long pullWatchId = 666L;
        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        RemovePullWatchResponse response;

        when(serverRpcMock.removePullWatch(any(RemovePullWatchRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(RemovePullWatchResponse::new));
        response = clientServerRpc.removePullWatch(new RemovePullWatchRequest(pullWatchId)).get();
        Assert.assertTrue(response.success());
        verify(serverRpcMock).removePullWatch(argThat((RemovePullWatchRequest r) -> r.getPullWatchId() == pullWatchId));
    }

    @Test
    public void testPullEvents() throws ExecutionException, InterruptedException {

        long pullWatchId = 666L;
        long ackSequence = 888888L;
        Map<String, String> eventData = new HashMap<>();
        eventData.put("key1", "value1");
        eventData.put("key2", "value2");
        List<PullEvent> pullEvents = Collections.singletonList(new PullEvent(23, 83999L, eventData));


        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());
        PullEventsResponse response;

        when(serverRpcMock.pullEvents(any(PullEventsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new PullEventsResponse(pullEvents)));
        response = clientServerRpc.pullEvents(new PullEventsRequest(pullWatchId, ackSequence)).get();
        Assert.assertTrue(response.success());

        Assert.assertEquals(pullEvents.size(),response.getPullEvents().size());
        Assert.assertEquals(pullEvents.get(0).getSequence(), response.getPullEvents().get(0).getSequence());
        Assert.assertEquals(pullEvents.get(0).getEventData(), response.getPullEvents().get(0).getEventData());

        verify(serverRpcMock).pullEvents(argThat((PullEventsRequest r) ->
                r.getPullWatchId() == pullWatchId &&
                r.getAckSequence() == ackSequence));
    }

    @Test
    public void testWatch() throws Exception {
        long pullWatchId = 666L;
        long pullIntervalMs = 100L;
        Map<String, String> eventData = new HashMap<>();
        eventData.put("key1", "value1");
        eventData.put("key2", "value2");
        List<PullEvent> pullEvents = Collections.singletonList(new PullEvent(23, 83999L, eventData));


        ClientServerRpc clientServerRpc = clientServerRpcAccessPoint.getClintServerRpc(serverRpcMock.serverUri());

        AtomicBoolean addWatchFinished = new AtomicBoolean(false);
        AtomicBoolean alreadySendEvents = new AtomicBoolean(false);
        when(serverRpcMock.pullEvents(any(PullEventsRequest.class)))
                .thenAnswer(invocation -> CompletableFuture.supplyAsync(() ->  {
                    if(addWatchFinished.get() && alreadySendEvents.compareAndSet(false, true)) {
                        return new PullEventsResponse(pullEvents);
                    } else {
                        return new PullEventsResponse(Collections.emptyList());
                    }
                }));
        when(serverRpcMock.addPullWatch())
                .thenReturn(CompletableFuture.supplyAsync(() -> new AddPullWatchResponse(pullWatchId, pullIntervalMs)));
        when(serverRpcMock.removePullWatch(any(RemovePullWatchRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(RemovePullWatchResponse::new));

        List<Event> eventList = new ArrayList<>();
        EventWatcher eventWatcher = eventList::add;

        clientServerRpc.watch(eventWatcher);
        addWatchFinished.set(true);
        Thread.sleep(3 * pullIntervalMs);
        clientServerRpc.unWatch(eventWatcher);

        Assert.assertEquals(pullEvents.size(), eventList.size());
        Assert.assertEquals(pullEvents.get(0).getEventData(), eventList.get(0).getEventData());
    }



    @Test
    public void testAsyncAppendEntries() throws ExecutionException, InterruptedException {

        AsyncAppendEntriesRequest request = new AsyncAppendEntriesRequest(
                88,
                URI.create("jk://leader.host:8888"),
                838472234228L,
                87,
                ByteUtils.createRandomSizeByteList(1024, 1000),
                6666666L,
                6666688L);
        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        AsyncAppendEntriesResponse response, serverResponse;
        serverResponse = new AsyncAppendEntriesResponse(false, 8837222L, 74,request.getEntries().size());
        // Test success response
        when(serverRpcMock.asyncAppendEntries(any(AsyncAppendEntriesRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.asyncAppendEntries(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.getJournalIndex(), response.getJournalIndex());
        Assert.assertEquals(serverResponse.isSuccess(), response.isSuccess());
        Assert.assertEquals(serverResponse.getTerm(), response.getTerm());
        Assert.assertEquals(serverResponse.getEntryCount(), response.getEntryCount());

        verify(serverRpcMock).asyncAppendEntries(
                argThat((AsyncAppendEntriesRequest r) ->
                                r.getTerm() == request.getTerm() &&
                                r.getLeader().equals(request.getLeader()) &&
                                r.getPrevLogIndex() == request.getPrevLogIndex() &&
                                r.getPrevLogTerm() == request.getPrevLogTerm() &&
                                r.getLeaderCommit() == request.getLeaderCommit() &&
                                r.getMaxIndex() == request.getMaxIndex() &&
                                testListOfBytesEquals(r.getEntries(), request.getEntries())
                        ));

    }

    @Test
    public void testRequestVote() throws ExecutionException, InterruptedException {

        RequestVoteRequest request = new RequestVoteRequest(
                88,
                URI.create("jk://candidate.host:8888"),
                6666666L,
                87,
                false);
        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        RequestVoteResponse response, serverResponse;
        serverResponse = new RequestVoteResponse(88, false);
        // Test success response
        when(serverRpcMock.requestVote(any(RequestVoteRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.requestVote(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.isVoteGranted(), response.isVoteGranted());
        Assert.assertEquals(serverResponse.getTerm(), response.getTerm());

        verify(serverRpcMock).requestVote(
                argThat((RequestVoteRequest r) ->
                                r.getTerm() == request.getTerm() &&
                                r.getCandidate().equals(request.getCandidate()) &&
                                r.getLastLogIndex() == request.getLastLogIndex() &&
                                r.getLastLogTerm() == request.getLastLogTerm() &&
                                r.isFromPreferredLeader() == request.isFromPreferredLeader()
                        ));

    }


    @Test
    public void testGetServerEntries() throws ExecutionException, InterruptedException {

        GetServerEntriesRequest request = new GetServerEntriesRequest(
                6666666L,
                87
                );
        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        GetServerEntriesResponse response, serverResponse;
        serverResponse = new GetServerEntriesResponse(
                ByteUtils.createRandomSizeByteList(2048, 1024),
                87783L
                , 9384884L);
        // Test success response
        when(serverRpcMock.getServerEntries(any(GetServerEntriesRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.getServerEntries(request).get();
        Assert.assertTrue(response.success());
        Assert.assertTrue(testListOfBytesEquals(serverResponse.getEntries(), response.getEntries()));
        Assert.assertEquals(serverResponse.getMinIndex(), response.getMinIndex());
        Assert.assertEquals(serverResponse.getLastApplied(), response.getLastApplied());

        verify(serverRpcMock).getServerEntries(
                argThat((GetServerEntriesRequest r) ->
                                r.getIndex() == request.getIndex() &&
                                r.getMaxSize() == request.getMaxSize()
                ));

        when(serverRpcMock.getServerEntries(any(GetServerEntriesRequest.class)))
                .thenThrow(new IndexUnderflowException());
        response = serverRpc.getServerEntries(request).get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.INDEX_UNDERFLOW, response.getStatusCode());

        when(serverRpcMock.getServerEntries(any(GetServerEntriesRequest.class)))
                .thenThrow(new IndexOverflowException());
        response = serverRpc.getServerEntries(request).get();
        Assert.assertFalse(response.success());
        Assert.assertEquals(StatusCode.INDEX_OVERFLOW, response.getStatusCode());


    }

    @Test
    public void testGetServerState() throws ExecutionException, InterruptedException {

        GetServerStateRequest request = new GetServerStateRequest(
                6666666L,
                -1
                );
        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        GetServerStateResponse response, serverResponse;
        serverResponse = new GetServerStateResponse(
                2342345L,
                883,
                899334545L,
                ByteUtils.createRandomSizeBytes(1024 * 1024 * 10),
                false, 666);
        // Test success response
        when(serverRpcMock.getServerState(any(GetServerStateRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.getServerState(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.getLastIncludedIndex(), response.getLastIncludedIndex());
        Assert.assertEquals(serverResponse.getLastIncludedTerm(), response.getLastIncludedTerm());
        Assert.assertEquals(serverResponse.getOffset(), response.getOffset());
        Assert.assertArrayEquals(serverResponse.getData(), response.getData());
        Assert.assertEquals(serverResponse.isDone(), response.isDone());
        Assert.assertEquals(serverResponse.getIteratorId(), response.getIteratorId());

        verify(serverRpcMock).getServerState(
                argThat((GetServerStateRequest r) ->
                                r.getLastIncludedIndex() == request.getLastIncludedIndex() &&
                                        r.getIteratorId() == request.getIteratorId()
                ));

    }

    @Test
    public void testDisableLeaderWrite() throws ExecutionException, InterruptedException {
        final long timeout = 666666L;
        final int term = 666;
        DisableLeaderWriteRequest request = new DisableLeaderWriteRequest(
                timeout, term);
        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        DisableLeaderWriteResponse response, serverResponse;
        serverResponse = new DisableLeaderWriteResponse(term);
        // Test success response
        when(serverRpcMock.disableLeaderWrite(any(DisableLeaderWriteRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.disableLeaderWrite(request).get();
        Assert.assertTrue(response.success());

        verify(serverRpcMock).disableLeaderWrite(
                argThat((DisableLeaderWriteRequest r) ->
                                r.getTimeoutMs() == request.getTimeoutMs() &&
                                r.getTerm() == request.getTerm()
                ));
    }

    @Test
    public void testCompleteTransaction() throws ExecutionException, InterruptedException {
        final UUID transactionId = UUID.randomUUID();
        final boolean commitOrAbort = false;

        CompleteTransactionRequest request = new CompleteTransactionRequest(transactionId, commitOrAbort);

        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        CompleteTransactionResponse response, serverResponse;
        serverResponse = new CompleteTransactionResponse();
        // Test success response
        when(serverRpcMock.completeTransaction(any(CompleteTransactionRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.completeTransaction(request).get();
        Assert.assertTrue(response.success());

        verify(serverRpcMock).completeTransaction(
                argThat((CompleteTransactionRequest r) ->
                                Objects.equals(r.getTransactionId(), request.getTransactionId()) &&
                                r.isCommitOrAbort() == request.isCommitOrAbort()
                ));
    }

    @Test
    public void testInstallSnapshot() throws ExecutionException, InterruptedException {
        // leader’s term
        final int term = 666;
        final int responseTerm = 888;

        // so follower can redirect clients
        final URI leaderId = URI.create("jk://localhost:8888");
        // the snapshot replaces all entries up through and including this index
        final long lastIncludedIndex = -1L;
        // term of lastIncludedIndex
        final int lastIncludedTerm = -1;
        // byte offset where chunk is positioned in the snapshot file
        final int offset = 0;
        // raw bytes of the snapshot chunk, starting at offset
        final byte[] data = ByteUtils.createFixedSizeBytes(1024);
        // true if this is the last chunk
        final boolean done = false;

        InstallSnapshotRequest request = new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done);

        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        InstallSnapshotResponse response, serverResponse;
        serverResponse = new InstallSnapshotResponse(responseTerm);
        // Test success response
        when(serverRpcMock.installSnapshot(any(InstallSnapshotRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.installSnapshot(request).get();
        Assert.assertTrue(response.success());

        verify(serverRpcMock).installSnapshot(
                argThat((InstallSnapshotRequest r) -> Objects.equals(request, r)
                ));
    }

    @Test
    public void testGetServerStatus() throws ExecutionException, InterruptedException {


        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        GetServerStatusResponse response, serverResponse;
        serverResponse = new GetServerStatusResponse(new ServerStatus(
                RaftServer.Roll.VOTER,
                0L,
                1024L,
                1000L,
                1020L,
                VoterState.LEADER
        ));
        // Test success response
        when(serverRpcMock.getServerStatus())
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.getServerStatus().get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.getServerStatus(), response.getServerStatus());

    }

    @Test
    public void testCreateTransaction() throws ExecutionException, InterruptedException {

        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        Map<String, String> context = new HashMap<>();
        context.put("aaa", "bbb");
        context.put("ccc", "dddd");
        CreateTransactionRequest request = new CreateTransactionRequest(context);
        CreateTransactionResponse response, serverResponse;
        serverResponse = new CreateTransactionResponse(new UUIDTransactionId(UUID.randomUUID()), System.currentTimeMillis());
        // Test success response
        when(serverRpcMock.createTransaction(any(CreateTransactionRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.createTransaction(request).get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.getTransactionId(), response.getTransactionId());

        verify(serverRpcMock).createTransaction(argThat(
                (CreateTransactionRequest r) -> r.getContext().equals(context)
        ));
    }

    @Test
    public void testGetOpeningTransactions() throws ExecutionException, InterruptedException {

        ServerRpc serverRpc = serverRpcAccessPoint.getServerRpcAgent(serverRpcMock.serverUri());
        GetOpeningTransactionsResponse response, serverResponse;

        Collection<JournalKeeperTransactionContext> contexts = IntStream.range(0, 3)
                .mapToObj(i -> {
                    Map<String, String> context = new HashMap<>();
                    context.put("key", String.valueOf(i));
                    return new JournalKeeperTransactionContext(
                            new UUIDTransactionId(UUID.randomUUID()),
                            context,
                            System.currentTimeMillis()
                    );
                }).collect(Collectors.toList());

        serverResponse = new GetOpeningTransactionsResponse(contexts);
        // Test success response
        when(serverRpcMock.getOpeningTransactions())
                .thenReturn(CompletableFuture.supplyAsync(() -> serverResponse));
        response = serverRpc.getOpeningTransactions().get();
        Assert.assertTrue(response.success());
        Assert.assertEquals(serverResponse.getTransactionContexts(), response.getTransactionContexts());

    }


    private static boolean testListOfBytesEquals(List<byte[]> entries, List<byte[]> entries1) {
        if(entries.size() == entries1.size()) {
            for (int i = 0; i < entries.size(); i++) {
                if(!Arrays.equals(entries.get(i), entries1.get(i))){
                    return false;
                }
            }
            return  true;
        }
        return false;
    }


    @After
    public void after() {
        if(null != server) server.stop();
    }





}
