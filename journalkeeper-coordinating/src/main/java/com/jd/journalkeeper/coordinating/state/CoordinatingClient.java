package com.jd.journalkeeper.coordinating.state;

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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * CoordinatingClient
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
// TODO 异常处理
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

    public boolean set(byte[] key, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.SET.getType(), key, value)).get();
            return true;
        } catch (Exception e) {
            logger.error("set exception, key: {}, value: {}", key, value, e);
            throw new RuntimeException(e);
        }
    }

    public boolean compareAndSet(byte[] key, byte[] expect, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.COMPARE_AND_SET.getType(), key, expect, value)).get();
            return true;
        } catch (Exception e) {
            logger.error("compareAndSet exception, key: {}, expect: {}, value: {}", key, expect, value, e);
            throw new RuntimeException(e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return doQuery(new StateReadRequest(StateTypes.GET.getType(), key))
                    .thenApply(StateResponse::getValue)
                    .get();
        } catch (Exception e) {
            logger.error("get exception, key: {}", key, e);
            throw new RuntimeException(e);
        }
    }

    public boolean remove(byte[] key) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.REMOVE.getType(), key)).get();
            return true;
        } catch (Exception e) {
            logger.error("remove exception. key: {}", key, e);
            throw new RuntimeException(e);
        }
    }

    public boolean exist(byte[] key) {
        try {
            return doQuery(new StateReadRequest(StateTypes.EXIST.getType(), key))
                    .thenApply(StateResponse::getValue)
                    .thenApply(response -> {
                        if (response[0] == 0) {
                            return false;
                        } else {
                            return true;
                        }
                    })
                    .get();
        } catch (Exception e) {
            logger.error("exist exception, key: {}", key, e);
            throw new RuntimeException(e);
        }
    }

    public URI getLeader() {
        try {
            return client.getServers().get().getLeader();
        } catch (Exception e) {
            logger.error("getLeader exception", e);
            return null;
        }
    }

    public void stop() {
        client.stop();
    }

    protected CompletableFuture<Void> doUpdate(StateWriteRequest request) {
        return client.update(request, 0, 1, ResponseConfig.REPLICATION);
    }

    protected CompletableFuture<StateResponse> doQuery(StateReadRequest request) {
        return client.query(request)
                .exceptionally(t -> {
                    throw new RuntimeException(t.getCause());
                }).thenApply(response -> {
                    if (response.getCode() != StateCodes.SUCCESS.getCode()) {
                        throw new RuntimeException(String.valueOf(StateCodes.valueOf(response.getCode())));
                    }
                    return response;
                });
    }
}