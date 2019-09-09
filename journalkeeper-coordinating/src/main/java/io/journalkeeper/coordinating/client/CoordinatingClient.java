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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.coordinating.client.exception.CoordinatingClientException;
import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.StateCodes;
import io.journalkeeper.coordinating.state.domain.StateTypes;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.ResponseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * CoordinatingClient
 * author: gaohaoxiang
 *
 * date: 2019/6/4
 */
// TODO response
public class CoordinatingClient {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingClient.class);

    private List<URI> servers;
    private Properties config;
    private RaftClient<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client;

    public CoordinatingClient(List<URI> servers,
                              Properties config,
                              RaftClient<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client) {
        this.servers = servers;
        this.config = config;
        this.client = client;
    }

    public CompletableFuture<WriteResponse> set(byte[] key, byte[] value) {
        return doUpdate(new WriteRequest(StateTypes.SET.getType(), key, value))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        return doQuery(new ReadRequest(StateTypes.GET.getType(), key))
                .exceptionally(cause -> {
                    throw convertException(cause);
                })
                .thenApply(ReadResponse::getValue);
    }

    public CompletableFuture<List<byte[]>> list(List<byte[]> keys) {
        return doQuery(new ReadRequest(StateTypes.LIST.getType(), new ArrayList<>(keys)))
                .thenApply(ReadResponse::getValues)
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<WriteResponse> compareAndSet(byte[] key, byte[] expect, byte[] value) {
        return doUpdate(new WriteRequest(StateTypes.COMPARE_AND_SET.getType(), key, expect, value))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<WriteResponse> remove(byte[] key) {
        return doUpdate(new WriteRequest(StateTypes.REMOVE.getType(), key))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Boolean> exist(byte[] key) {
        return doQuery(new ReadRequest(StateTypes.EXIST.getType(), key))
                .exceptionally(cause -> {
                    throw convertException(cause);
                })
                .thenApply(ReadResponse::getValue)
                .thenApply(response -> response[0] == 1);
    }

    public void watch(CoordinatingEventListener listener) {
        client.watch(new EventWatcherAdapter(listener));
    }

    public void unwatch(CoordinatingEventListener listener) {
        client.unWatch(new EventWatcherAdapter(listener));
    }

    public void watch(byte[] key, CoordinatingEventListener listener) {
        client.watch(new EventWatcherAdapter(key, listener));
    }

    public void unwatch(byte[] key, CoordinatingEventListener listener) {
        client.unWatch(new EventWatcherAdapter(key, listener));
    }

    public CompletableFuture waitClusterReady(long maxWaitMs) {
        return this.client.waitClusterReady(maxWaitMs);
    }
    public void stop() {
        client.stop();
    }

    protected CoordinatingClientException convertException(Throwable cause) {
        if (cause instanceof CoordinatingClientException) {
            return (CoordinatingClientException) cause;
        } else if (cause instanceof ExecutionException) {
            return new CoordinatingClientException(cause.getCause());
        } else {
            throw new CoordinatingClientException(cause);
        }
    }

    protected CompletableFuture<WriteResponse> doUpdate(WriteRequest request) {
        return client.update(request, 0, 1, ResponseConfig.REPLICATION);
    }

    protected CompletableFuture<ReadResponse> doQuery(ReadRequest request) {
        return client.query(request)
                .exceptionally(t -> {
                    throw new CoordinatingClientException(t.getCause());
                }).thenApply(response -> {
                    if (response.getCode() != StateCodes.SUCCESS.getCode()) {
                        throw new CoordinatingClientException(String.format("code: %s, msg: %s", String.valueOf(StateCodes.valueOf(response.getCode())), response.getMsg()));
                    }
                    return response;
                });
    }
}