package com.jd.journalkeeper.coordinating.test;

import com.jd.journalkeeper.coordinating.state.CoordinatingStateServer;
import com.jd.journalkeeper.coordinating.state.config.StateConfigs;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class CoordinatingStateServerTest {

    private CoordinatingStateServer server;

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty(StateConfigs.STORE_TYPE, "rocksdb");
        properties.setProperty("working_dir", "/Users/gaohaoxiang/export/rocksdb");

        URI current = URI.create("journalkeeper://127.0.0.1:50088");
        List<URI> voters = Arrays.asList(current);
        server = new CoordinatingStateServer(current, voters, properties);
        server.start();

        server.waitForLeaderReady(1000 * 10, TimeUnit.MILLISECONDS);
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
                        metrics.getTp99(), metrics.getTp90(), metrics.getAvg(), metrics.getMax(), metrics.getOneMinuteRate(), metrics.getCount()));
            }
        }).start();

        byte[] key = RandomStringUtils.randomAlphanumeric(10).getBytes();
        byte[] value = RandomStringUtils.randomAlphanumeric(10).getBytes();

        while (true) {
            long now = System.currentTimeMillis();

            server.put(key, value);

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
        server.stop();
    }
}