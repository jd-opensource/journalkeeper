package com.jd.journalkeeper.coordinating.test.cluster;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.keeper.config.KeeperConfigs;
import com.jd.journalkeeper.core.api.RaftServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public abstract class AbstractStateServerTest {

    private static final int NODES = 4;
    private static final int BASE_PORT = 50088;
    private CoordinatingKeeperServer server;

    @Before
    public void before() {
        List<URI> voters = new ArrayList<>();

        for (int i = 0; i < NODES; i++) {
            voters.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        Properties properties = new Properties();
        properties.setProperty(KeeperConfigs.STATE_STORE, "rocksdb");
        properties.setProperty("working_dir", String.format("/Users/gaohaoxiang/export/rocksdb/%s", getIndex()));

        URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + getIndex())));
        CoordinatingKeeperServer server = new CoordinatingKeeperServer(current, voters, RaftServer.Roll.VOTER, properties);
        server.start();

        this.server = server;
    }

    @Test
    public void test() {
        try {
            System.in.read();
        } catch (IOException e) {
        }
    }

    protected abstract int getIndex();
}