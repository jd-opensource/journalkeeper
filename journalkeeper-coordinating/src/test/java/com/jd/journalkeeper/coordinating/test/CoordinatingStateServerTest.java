package com.jd.journalkeeper.coordinating.test;

import com.jd.journalkeeper.coordinating.state.CoordinatingStateServer;
import com.jd.journalkeeper.coordinating.state.config.StateConfigs;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class CoordinatingStateServerTest {

    private static final int NODES = 3;
    private static final int BASE_PORT = 50088;
    private static final int KEY_LENGTH = 1024;
    private static final int VALUE_LENGTH = 1024;
    private List<CoordinatingStateServer> servers = new ArrayList<>();

    @Before
    public void before() {
        List<URI> voters = new ArrayList<>();

        for (int i = 0; i < NODES; i++) {
            voters.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        for (int i = 0; i < NODES; i++) {
            Properties properties = new Properties();
            properties.setProperty(StateConfigs.STORE_TYPE, "rocksdb");
            properties.setProperty("working_dir", String.format("/Users/gaohaoxiang/export/rocksdb/%s", i));

            URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i)));
            CoordinatingStateServer server = new CoordinatingStateServer(current, voters, properties);
            server.start();
            servers.add(server);
        }

        servers.get(0).waitForLeaderReady(1000 * 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test() {
        Metrics metrics = new Metrics();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.currentThread().sleep(1000 * 1);
                } catch (InterruptedException e) {
                }

                System.out.println(String.format("tp99: %s, tp90: %s, avg: %s, max: %s, tps: %s, total: %s",
                        metrics.getTp99(), metrics.getTp90(), metrics.getAvg(), metrics.getMax(), metrics.getMeter().getMeanRate(), metrics.getCount()));
            }
        }).start();

        while (true) {

            byte[] key = RandomStringUtils.randomAlphanumeric(KEY_LENGTH).getBytes();
            byte[] value = RandomStringUtils.randomAlphanumeric(VALUE_LENGTH).getBytes();

            long now = System.currentTimeMillis();

            servers.get((int) System.currentTimeMillis() % servers.size()).put(key, value);

//            Assert.assertArrayEquals(value, server.get(key));
//
//            server.remove(key);
//
//            Assert.assertEquals(server.get(key), null);

            metrics.mark(System.currentTimeMillis() - now, 1);
        }
    }

    @After
    public void after() {
        for (CoordinatingStateServer server : servers) {
            server.stop();
        }
    }
}