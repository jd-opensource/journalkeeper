package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * CoordinatingServer
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class CoordinatingServer implements StateServer {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingServer.class);

    private URI current;
    private List<URI> servers;
    private RaftServer.Roll role;
    private Properties config;

    private BootStrap bootStrap;
    private volatile CoordinatingClient client;

    public CoordinatingServer(URI current, List<URI> servers, Properties config,
                              RaftServer.Roll role,
                              StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory,
                              Serializer<StateWriteRequest> entrySerializer,
                              Serializer<StateReadRequest> querySerializer,
                              Serializer<StateResponse> resultSerializer) {
        this.current = current;
        this.servers = servers;
        this.role = role;
        this.config = config;
        this.bootStrap = new BootStrap<>(role, stateFactory, entrySerializer, querySerializer, resultSerializer, config);
    }

    public boolean waitForLeaderReady(int timeout, TimeUnit unit) {
        long timeoutLine = System.currentTimeMillis() + unit.toMillis(timeout);

        while (timeoutLine > System.currentTimeMillis()) {
            URI leader = getLeader();

            if (leader != null) {
                return true;
            }

            try {
                Thread.currentThread().sleep(10);
            } catch (InterruptedException e) {
            }
        }

        return false;
    }

    public URI getCurrent() {
        return current;
    }

    public List<URI> getServers() {
        return servers;
    }

    public RaftServer.Roll getRole() {
        return role;
    }

    public URI getLeader() {
        return getClient().getLeader();
    }

    public CoordinatingClient getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = new CoordinatingClient(servers, config, bootStrap.getClient());
                }
            }
        }
        return client;
    }

    @Override
    public void start() {
        try {
            bootStrap.getServer().init(current, servers);
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