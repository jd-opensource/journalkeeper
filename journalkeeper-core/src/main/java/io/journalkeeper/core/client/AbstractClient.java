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
package io.journalkeeper.core.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.ClusterReadyAware;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.SerializedUpdateRequest;
import io.journalkeeper.core.api.ServerConfigAware;
import io.journalkeeper.rpc.*;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.event.Watchable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-09-09
 */
public abstract class AbstractClient implements ClusterReadyAware, ServerConfigAware, Watchable {
    final ClientRpc clientRpc;
    private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);
    AbstractClient(ClientRpc clientRpc) {
        this.clientRpc = clientRpc;
    }

    protected <R> CompletableFuture<List<R>> update(List<SerializedUpdateRequest> entries, boolean includeHeader, ResponseConfig responseConfig, Serializer<R> serializer) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(entries, includeHeader, responseConfig)))
                        .thenApply(this::checkResponse)
                        .thenApply(UpdateClusterStateResponse::getResults)
                        .thenApply(serializedResults ->
                                serializedResults.stream().map(serializer::parse).collect(Collectors.toList())
                                );
    }
    protected <R> CompletableFuture<List<R>> update(List<SerializedUpdateRequest> entries, ResponseConfig responseConfig, Serializer<R> serializer) {
        return update(entries, false, responseConfig, serializer);
    }

    <R extends BaseResponse> R  checkResponse(R response) {
        if (response.getStatusCode() != StatusCode.SUCCESS) {
            throw new RpcException(response);
        }
        return response;
    }

    @Override
    public void waitForClusterReady(long maxWaitMs) throws TimeoutException {


            long t0 = System.currentTimeMillis();
            while (System.currentTimeMillis() - t0 < maxWaitMs || maxWaitMs <= 0) {
                try {
                    GetServersResponse response = maxWaitMs > 0 ?
                            clientRpc.invokeClientLeaderRpc(ClientServerRpc::getServers)
                                    .get(maxWaitMs, TimeUnit.MILLISECONDS) :
                            clientRpc.invokeClientLeaderRpc(ClientServerRpc::getServers)
                                    .get();
                    if(response.success()) {
                        return;
                    }
                } catch (Exception e) {
                    logger.info("Query servers failed. Error: {}", e.getMessage());
                }
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(100L));
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }
            }
            throw new TimeoutException();

    }

    @Override
    public void updateServers(List<URI> servers) {
        clientRpc.updateServers(servers);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        clientRpc.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        clientRpc.unWatch(eventWatcher);
    }

    public void stop(){
        clientRpc.stop();
    }
}
