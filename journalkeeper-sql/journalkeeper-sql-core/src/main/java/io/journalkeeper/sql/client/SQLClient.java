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
package io.journalkeeper.sql.client;

import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.sql.client.domain.Codes;
import io.journalkeeper.sql.client.domain.OperationTypes;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.Response;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.exception.SQLClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * SQLClient
 * author: gaohaoxiang
 * date: 2019/6/4
 */
public class SQLClient {

    protected static final Logger logger = LoggerFactory.getLogger(SQLClient.class);

    private List<URI> servers;
    private Properties config;
    private RaftClient<WriteRequest, ReadRequest, Response> client;

    public SQLClient(List<URI> servers,
                     Properties config,
                     RaftClient<WriteRequest, ReadRequest, Response> client) {
        this.servers = servers;
        this.config = config;
        this.client = client;
    }

    public CompletableFuture<List<Map<String, String>>> query(String id, String sql, String... params) {
        return doQuery(new ReadRequest(OperationTypes.QUERY.getType(), id, sql, params))
                .exceptionally(cause -> {
                    throw convertException(cause);
                }).thenApply(Response::getRows);
    }

    public CompletableFuture<List<Map<String, String>>> query(String sql, String... params) {
        return query(null, sql, params);
    }

    public CompletableFuture<Void> insert(String id, String sql, String... params) {
        return doUpdate(new WriteRequest(OperationTypes.INSERT.getType(), id, sql, params))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Void> insert(String sql, String... params) {
        return insert(null, sql, params);
    }

    public CompletableFuture<Void> update(String id, String sql, String... params) {
        return doUpdate(new WriteRequest(OperationTypes.UPDATE.getType(), id, sql, params))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Void> update(String sql, String... params) {
        return update(null, sql, params);
    }

    public CompletableFuture<Void> delete(String id, String sql, String... params) {
        return doUpdate(new WriteRequest(OperationTypes.DELETE.getType(), id, sql, params))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Void> delete(String sql, String... params) {
        return delete(null, sql, params);
    }

    public CompletableFuture<Void> beginTransaction(String id) {
        return doUpdate(new WriteRequest(OperationTypes.TRANSACTION_BEGIN.getType(), id))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Void> commitTransaction(String id) {
        return doUpdate(new WriteRequest(OperationTypes.TRANSACTION_COMMIT.getType(), id))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public CompletableFuture<Void> rollbackTransaction(String id) {
        return doUpdate(new WriteRequest(OperationTypes.TRANSACTION_ROLLBACK.getType(), id))
                .exceptionally(cause -> {
                    throw convertException(cause);
                });
    }

    public void watch(SQLEventListener listener) {
        client.watch(new EventWatcherAdapter(listener));
    }

    public void unwatch(SQLEventListener listener) {
        client.unWatch(new EventWatcherAdapter(listener));
    }

    public void watch(byte[] key, SQLEventListener listener) {
        client.watch(new EventWatcherAdapter(key, listener));
    }

    public void unwatch(byte[] key, SQLEventListener listener) {
        client.unWatch(new EventWatcherAdapter(key, listener));
    }

    public URI getLeader() {
        try {
            return client.getServers().get().getLeader();
        } catch (Exception e) {
            throw new SQLClientException(e);
        }
    }

    public void stop() {
        client.stop();
    }

    protected SQLClientException convertException(Throwable cause) {
        if (cause instanceof SQLClientException) {
            return (SQLClientException) cause;
        } else if (cause instanceof ExecutionException) {
            return new SQLClientException(cause.getCause());
        } else {
            throw new SQLClientException(cause);
        }
    }

    protected CompletableFuture<Void> doUpdate(WriteRequest request) {
        return client.update(request, 0, 1, ResponseConfig.REPLICATION);
    }

    protected CompletableFuture<Response> doQuery(ReadRequest request) {
        return client.query(request)
                .exceptionally(t -> {
                    throw new SQLClientException(t.getCause());
                }).thenApply(response -> {
                    if (response.getCode() != Codes.SUCCESS.getCode()) {
                        throw new SQLClientException(String.format("code: %s, msg: %s",
                                String.valueOf(Codes.valueOf(response.getCode())), response.getMsg()));
                    }
                    return response;
                });
    }
}