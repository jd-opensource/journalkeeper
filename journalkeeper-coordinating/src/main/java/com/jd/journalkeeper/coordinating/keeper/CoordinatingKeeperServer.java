package com.jd.journalkeeper.coordinating.keeper;

import com.jd.journalkeeper.coordinating.keeper.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.keeper.domain.StateResponse;
import com.jd.journalkeeper.coordinating.keeper.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.keeper.serializer.KryoSerializer;
import com.jd.journalkeeper.coordinating.keeper.state.CoordinatorStateFactory;
import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * CoordinatingKeeperServer
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
// TODO 异常处理
public class CoordinatingKeeperServer implements StateServer {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingKeeperServer.class);

    private URI current;
    private List<URI> cluster;
    private RaftServer.Roll role;
    private BootStrap bootStrap;

    private CoordinatingKeeperState state;

    public CoordinatingKeeperServer(URI current, List<URI> cluster, RaftServer.Roll role, Properties config) {
        this.current = current;
        this.cluster = cluster;
        this.role = role;
        this.bootStrap = new BootStrap<>(role,
                        new CoordinatorStateFactory(),
                        new KryoSerializer(StateWriteRequest.class),
                        new KryoSerializer(StateReadRequest.class),
                        new KryoSerializer(StateResponse.class), config);
        this.state = new CoordinatingKeeperState(bootStrap, config);
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

    public List<URI> getCluster() {
        return cluster;
    }

    public RaftServer.Roll getRole() {
        return role;
    }

    public URI getLeader() {
        return getState().getLeader();
    }

    public CoordinatingKeeperState getState() {
        return state;
    }

    @Override
    public void start() {
        try {
            bootStrap.getServer().init(current, cluster);
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