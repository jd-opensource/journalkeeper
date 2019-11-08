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
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.ResultSet;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.client.exception.SQLClientException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * SQLClient
 * author: gaohaoxiang
 * date: 2019/6/4
 */
public class SQLClient {

    protected static final Logger logger = LoggerFactory.getLogger(SQLClient.class);

    private List<URI> servers;
    private Properties config;
    private RaftClient<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client;

    public SQLClient(List<URI> servers,
                     Properties config,
                     RaftClient<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client) {
        this.servers = servers;
        this.config = config;
        this.client = client;
    }

//    public CompletableFuture waitClusterReady(long maxWaitMs) {
//        return this.client.whenClusterReady(maxWaitMs);
//    }

    public void waitClusterReady(Long maxWaitMs) throws TimeoutException, InterruptedException {
        this.client.waitForClusterReady(maxWaitMs);
    }
    public CompletableFuture<ResultSet> query(String sql, List<Object> params) {
        try {
            return doQuery(new ReadRequest(OperationTypes.QUERY.getType(), sql, params))
                    .exceptionally(cause -> {
                        throw convertException(cause);
                    }).thenApply(ReadResponse::getResultSet);
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public CompletableFuture<Object> insert(String sql, List<Object> params) {
        try {
            return doUpdate(new WriteRequest(OperationTypes.INSERT.getType(), sql, params))
                    .exceptionally(cause -> {
                        throw convertException(cause);
                    }).thenApply(response -> {
                        return response.getResult();
                    });
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public CompletableFuture<Object> update(String sql, List<Object> params) {
        try {
            return doUpdate(new WriteRequest(OperationTypes.UPDATE.getType(), sql, params))
                    .exceptionally(cause -> {
                        throw convertException(cause);
                    }).thenApply(response -> {
                        return Integer.valueOf(response.getResult().toString());
                    });
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public CompletableFuture<Object> delete(String sql, List<Object> params) {
        try {
            return doUpdate(new WriteRequest(OperationTypes.DELETE.getType(), sql, params))
                    .exceptionally(cause -> {
                        throw convertException(cause);
                    }).thenApply(response -> {
                        return Integer.valueOf(response.getResult().toString());
                    });
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public CompletableFuture<List<Object>> batch(List<String> sqlList, List<List<Object>> paramList) {
        try {
            return doUpdate(new WriteRequest(OperationTypes.BATCH.getType(), sqlList, paramList))
                    .exceptionally(cause -> {
                        throw convertException(cause);
                    }).thenApply(response -> {
                        return response.getResultList();
                    });
        } catch (Exception e) {
            throw convertException(e);
        }
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

    public void stop() {
        client.stop();
    }

    protected SQLClientException convertException(Throwable cause) {
        if (cause instanceof SQLClientException) {
            SQLClientException sqlClientException = (SQLClientException) cause;
            if (StringUtils.isBlank(sqlClientException.getMessage())) {
                return sqlClientException;
            } else {
                return new SQLClientException(sqlClientException.getMessage());
            }
        } else if (cause instanceof ExecutionException) {
            return new SQLClientException(cause.getCause());
        } else {
            throw new SQLClientException(cause);
        }
    }

    protected CompletableFuture<WriteResponse> doUpdate(WriteRequest request) {
        return client.update(request, 0, 1, ResponseConfig.REPLICATION).exceptionally(t -> {
            throw new SQLClientException(t.getCause());
        }).thenApply(response -> {
            if (response.getCode() != Codes.SUCCESS.getCode()) {
                throw new SQLClientException(String.format("code: %s, msg: %s",
                        String.valueOf(Codes.valueOf(response.getCode())), response.getMsg()));
            }
            return response;
        });
    }

    protected CompletableFuture<ReadResponse> doQuery(ReadRequest request) {
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