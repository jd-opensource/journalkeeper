package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.coordinating.state.domain.StateCodes;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateTypes;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.serializer.KryoSerializer;
import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.RaftClient;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * CoordinatingStateServer
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
// TODO 异常处理
public class CoordinatingStateServer implements StateServer {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingStateServer.class);

    private URI current;
    private List<URI> voters;
    private BootStrap bootStrap;

    private volatile RaftClient<StateWriteRequest, StateReadRequest, StateResponse> client;

    public CoordinatingStateServer(URI current, List<URI> voters, Properties properties) {
        this.current = current;
        this.voters = voters;
        this.bootStrap =
                new BootStrap<>(RaftServer.Roll.VOTER,
                        new CoordinatorStateFactory(),
                        new KryoSerializer(StateWriteRequest.class),
                        new KryoSerializer(StateReadRequest.class),
                        new KryoSerializer(StateResponse.class), properties);
    }

    public boolean waitForLeaderReady(int timeout, TimeUnit unit) {
        long timeoutLine = System.currentTimeMillis() + unit.toMillis(timeout);
        URI leader = null;
        while (leader == null) {
            try {
                leader = getOrCreateClient().getServers().get().getLeader();
                Thread.sleep(10);

                if (System.currentTimeMillis() > timeoutLine) {
                    return false;
                }
            } catch (Exception e) {
            }
        }

        return true;
    }

    public boolean put(byte[] key, byte[] value) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.PUT.getType(), key, value)).get();
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return doQuery(new StateReadRequest(StateTypes.GET.getType(), key))
                    .thenApply(StateResponse::getValue)
                    .get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean remove(byte[] key) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.REMOVE.getType(), key)).get();
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO 有问题
    public boolean compareAndSet(byte[] key, byte[] expect, byte[] update) {
        try {
            doUpdate(new StateWriteRequest(StateTypes.COMPARE_AND_SET.getType(), key, expect, update)).get();
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    @Override
    public void start() {
        try {
            bootStrap.getServer().init(current, voters);
            bootStrap.getServer().recover();
            bootStrap.getServer().start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        bootStrap.shutdown();
    }

    @Override
    public ServerState serverState() {
        return bootStrap.getServer().serverState();
    }
}