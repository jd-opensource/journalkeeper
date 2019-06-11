package com.jd.journalkeeper.coordinating.client;

import com.jd.journalkeeper.coordinating.client.exception.CoordinatingClientException;
import com.jd.journalkeeper.coordinating.state.domain.StateCodes;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateTypes;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.core.api.RaftClient;
import com.jd.journalkeeper.core.api.ResponseConfig;
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
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingClient {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingClient.class);

    private List<URI> servers;
    private Properties config;
    private RaftClient<StateWriteRequest, StateReadRequest, StateResponse> client;

    public CoordinatingClient(List<URI> servers,
                              Properties config,
                              RaftClient<StateWriteRequest, StateReadRequest, StateResponse> client) {
        this.servers = servers;
        this.config = config;
        this.client = client;
    }

    public void set(byte[] key, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.SET.getType(), key, value)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return doQuery(new StateReadRequest(StateTypes.GET.getType(), key))
                    .thenApply(StateResponse::getValue)
                    .get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public List<byte[]> list(List<byte[]> keys) {
        try {
            return doQuery(new StateReadRequest(StateTypes.LIST.getType(), new ArrayList<>(keys)))
                    .thenApply(StateResponse::getValues)
                    .get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public void compareAndSet(byte[] key, byte[] expect, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.COMPARE_AND_SET.getType(), key, expect, value)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public void remove(byte[] key) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.REMOVE.getType(), key)).get();
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    public boolean exist(byte[] key) {
        try {
            return doQuery(new StateReadRequest(StateTypes.EXIST.getType(), key))
                    .thenApply(StateResponse::getValue)
                    .thenApply(response -> response[0] == 1)
                    .get();
        } catch (Exception e) {
            throw convertException(e);
        }
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

    public URI getLeader() {
        try {
            return client.getServers().get().getLeader();
        } catch (Exception e) {
            throw new CoordinatingClientException(e);
        }
    }

    public void stop() {
        client.stop();
    }

    protected CoordinatingClientException convertException(Exception cause) {
        if (cause instanceof CoordinatingClientException) {
            return (CoordinatingClientException) cause;
        } else if (cause instanceof ExecutionException) {
            return new CoordinatingClientException(cause.getCause());
        } else {
            throw new CoordinatingClientException(cause);
        }
    }

    protected CompletableFuture<Void> doUpdate(StateWriteRequest request) {
        return client.update(request, 0, 1, ResponseConfig.REPLICATION);
    }

    protected CompletableFuture<StateResponse> doQuery(StateReadRequest request) {
        return client.query(request)
                .exceptionally(t -> {
                    throw new CoordinatingClientException(t.getCause());
                }).thenApply(response -> {
                    if (response.getCode() != StateCodes.SUCCESS.getCode()) {
                        throw new CoordinatingClientException(String.valueOf(StateCodes.valueOf(response.getCode())));
                    }
                    return response;
                });
    }
}