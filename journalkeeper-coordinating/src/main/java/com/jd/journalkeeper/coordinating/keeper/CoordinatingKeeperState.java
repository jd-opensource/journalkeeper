package com.jd.journalkeeper.coordinating.keeper;

import com.jd.journalkeeper.coordinating.keeper.domain.StateCodes;
import com.jd.journalkeeper.coordinating.keeper.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.keeper.domain.StateResponse;
import com.jd.journalkeeper.coordinating.keeper.domain.StateTypes;
import com.jd.journalkeeper.coordinating.keeper.domain.StateWriteRequest;
import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.RaftClient;
import com.jd.journalkeeper.core.api.ResponseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * CoordinatingKeeperState
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
// TODO 异常处理
public class CoordinatingKeeperState {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingKeeperState.class);

    private BootStrap bootStrap;
    private Properties config;

    private volatile RaftClient<StateWriteRequest, StateReadRequest, StateResponse> client;

    public CoordinatingKeeperState(BootStrap bootStrap, Properties config) {
        this.bootStrap = bootStrap;
        this.config = config;
    }

    public boolean put(byte[] key, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.PUT.getType(), key, value)).get();
            return true;
        } catch (Exception e) {
            logger.error("put exception, key: {}, value: {}", key, value, e);
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
            return doQuery(new StateReadRequest(StateTypes.GET.getType(), key))
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
            return getOrCreateClient().getServers().get().getLeader();
        } catch (Exception e) {
            logger.error("getLeader exception", e);
            return null;
        }
    }

    protected CompletableFuture<Void> doUpdate(StateWriteRequest request) {
        return getOrCreateClient().update(request, 0, 1, ResponseConfig.REPLICATION);
    }

    protected CompletableFuture<StateResponse> doQuery(StateReadRequest request) {
        return getOrCreateClient().query(request)
                .exceptionally(t -> {
                    throw new RuntimeException(t.getCause());
                }).thenApply(response -> {
                    if (response.getCode() != StateCodes.SUCCESS.getCode()) {
                        throw new RuntimeException(String.valueOf(StateCodes.valueOf(response.getCode())));
                    }
                    return response;
                });
    }

    protected RaftClient<StateWriteRequest, StateReadRequest, StateResponse> getOrCreateClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = bootStrap.getClient();
                }
            }
        }
        return client;
    }
}