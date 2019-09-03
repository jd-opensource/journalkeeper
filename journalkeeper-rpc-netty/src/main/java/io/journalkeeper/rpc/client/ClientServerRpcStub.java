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
package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.utils.CommandSupport;
import io.journalkeeper.utils.event.EventBus;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.threads.AsyncLoopThread;
import io.journalkeeper.utils.threads.ThreadBuilder;
import io.journalkeeper.rpc.remoting.transport.TransportState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端桩
 * @author LiYue
 * Date: 2019-03-30
 */
public class ClientServerRpcStub implements ClientServerRpc {
    private static final Logger logger = LoggerFactory.getLogger(ClientServerRpcStub.class);
    protected final Transport transport;
    protected final URI uri;
    protected EventBus eventBus = null;
    protected AsyncLoopThread pullEventThread = null;
    protected long pullWatchId = -1L;
    protected long ackSequence = -1L;
    public ClientServerRpcStub(Transport transport, URI uri) {
        this.transport = transport;
        this.uri = uri;
    }


    @Override
    public URI serverUri() {
        return uri;
    }

    @Override
    public CompletableFuture<UpdateClusterStateResponse> updateClusterState(UpdateClusterStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.UPDATE_CLUSTER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryClusterState(QueryStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.QUERY_CLUSTER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<QueryStateResponse> queryServerState(QueryStateRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.QUERY_SERVER_STATE_REQUEST, transport);
    }

    @Override
    public CompletableFuture<LastAppliedResponse> lastApplied() {
        return CommandSupport.sendRequest(null, RpcTypes.LAST_APPLIED_REQUEST, transport);

    }

    @Override
    public CompletableFuture<QueryStateResponse> querySnapshot(QueryStateRequest request) {
        return CommandSupport
                .sendRequest(request, RpcTypes.QUERY_SNAPSHOT_REQUEST, transport);
    }

    @Override
    public CompletableFuture<GetServersResponse> getServers() {
        return CommandSupport.sendRequest(null, RpcTypes.GET_SERVERS_REQUEST, transport);
    }

    @Override
    public CompletableFuture<AddPullWatchResponse> addPullWatch() {
        return CommandSupport.sendRequest(null, RpcTypes.ADD_PULL_WATCH_REQUEST, transport);
    }

    @Override
    public CompletableFuture<RemovePullWatchResponse> removePullWatch(RemovePullWatchRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.REMOVE_PULL_WATCH_REQUEST, transport);
    }

    @Override
    public CompletableFuture<UpdateVotersResponse> updateVoters(UpdateVotersRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.UPDATE_VOTERS_REQUEST, transport);
    }

    @Override
    public CompletableFuture<PullEventsResponse> pullEvents(PullEventsRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.PULL_EVENTS_REQUEST, transport);
    }

    @Override
    public CompletableFuture<ConvertRollResponse> convertRoll(ConvertRollRequest request) {
        return CommandSupport.sendRequest(request, RpcTypes.CONVERT_ROLL_REQUEST, transport);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        if(null == eventBus) {
            initPullEvent();
        }
        eventBus.watch(eventWatcher);
    }

    private void initPullEvent() {
        try {
            AddPullWatchResponse addPullWatchResponse = addPullWatch().get();
            if (addPullWatchResponse.success()) {
                this.pullWatchId = addPullWatchResponse.getPullWatchId();
                this.ackSequence = -1L;
                long pullInterval = addPullWatchResponse.getPullIntervalMs();
                pullEventThread = buildPullEventsThread(pullInterval);
                pullEventThread.start();
                eventBus = new EventBus();
            } else {
                throw new RpcException(addPullWatchResponse);
            }
        } catch (Throwable t) {
            throw new RpcException(t);
        }

    }

    private AsyncLoopThread buildPullEventsThread(long pullInterval) {
        return ThreadBuilder.builder()
                .name("PullEventsThread")
                .doWork(this::pullRemoteEvents)
                .sleepTime(pullInterval, pullInterval)
                .onException(e -> logger.warn("PullEventsThread Exception: ", e))
                .daemon(true)
                .build();
    }

    private void pullRemoteEvents() {
        pullEvents(new PullEventsRequest(pullWatchId, ackSequence))
                .thenAccept(response -> {
                    if(response.success()) {
                        if(null != response.getPullEvents()) {
                            response.getPullEvents().forEach(pullEvent -> {
                                eventBus.fireEvent(pullEvent);
                                ackSequence = pullEvent.getSequence();
                            });
                        }
                    } else {
                        logger.warn("Pull event error: {}", response.getError());
                    }
                });
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        if(null != eventBus) {
            eventBus.unWatch(eventWatcher);
            if(!eventBus.hasEventWatchers()) {
                destroyPullEvent();
            }
        }

    }

    private void destroyPullEvent() {
        if(null != eventBus) {
            eventBus.shutdown();
            eventBus = null;
        }
        if(null != pullEventThread) {
            pullEventThread.stop();
            eventBus = null;
        }
        if(pullWatchId >= 0) {
            try {
                RemovePullWatchResponse response = removePullWatch(new RemovePullWatchRequest(pullWatchId))
                        .get();
                if(!response.success()) {
                    throw new RpcException(response);
                }
            } catch (Throwable t) {
                logger.warn("Remove pull watch exception: ", t);
            } finally {
                pullWatchId = -1L;
            }
        }

    }

    @Override
    public boolean isAlive() {
        return null != transport  && transport.state() == TransportState.CONNECTED;
    }

    @Override
    public void stop() {
        if(null != transport) {
            transport.stop();
        }
        destroyPullEvent();
    }
}
