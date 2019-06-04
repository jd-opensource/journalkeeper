package com.jd.journalkeeper.coordinating.test;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.keeper.config.KeeperConfigs;
import com.jd.journalkeeper.core.api.RaftServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
public class KeeperServerTest {

//    2019-06-03 15:20:25 - WARN - LeaderReplicationThread - com.jd.journalkeeper.core.server.Voter.lambda$buildLeaderReplicationThread$5(Voter.java:194) - LeaderReplicationThread Exception, voterState: FOLLOWER, currentTerm: 2, minIndex: 0, maxIndex: 2, commitIndex: 0, lastApplied: 0, uri: journalkeeper://127.0.0.1:50089:
//    java.util.ConcurrentModificationException: null
//    at java.util.ArrayList$ArrayListSpliterator.forEachRemaining(ArrayList.java:1388) ~[?:1.8.0_161]
//    at java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:481) ~[?:1.8.0_161]
//    at java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:471) ~[?:1.8.0_161]
//    at java.util.stream.ReduceOps$ReduceTask.doLeaf(ReduceOps.java:747) ~[?:1.8.0_161]
//    at java.util.stream.ReduceOps$ReduceTask.doLeaf(ReduceOps.java:721) ~[?:1.8.0_161]
//    at java.util.stream.AbstractTask.compute(AbstractTask.java:316) ~[?:1.8.0_161]
//    at java.util.concurrent.CountedCompleter.exec(CountedCompleter.java:731) ~[?:1.8.0_161]
//    at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289) ~[?:1.8.0_161]
//    at java.util.concurrent.ForkJoinTask.doInvoke(ForkJoinTask.java:401) ~[?:1.8.0_161]
//    at java.util.concurrent.ForkJoinTask.invoke(ForkJoinTask.java:734) ~[?:1.8.0_161]
//    at java.util.stream.ReduceOps$ReduceOp.evaluateParallel(ReduceOps.java:714) ~[?:1.8.0_161]
//    at java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:233) ~[?:1.8.0_161]
//    at java.util.stream.IntPipeline.reduce(IntPipeline.java:456) ~[?:1.8.0_161]
//    at java.util.stream.IntPipeline.sum(IntPipeline.java:414) ~[?:1.8.0_161]
//    at com.jd.journalkeeper.core.server.Voter.replication(Voter.java:411) ~[classes/:?]
//    at com.jd.journalkeeper.utils.threads.LoopThread$Builder$1.doWork(LoopThread.java:214) [classes/:?]
//    at com.jd.journalkeeper.utils.threads.LoopThread.run(LoopThread.java:102) [classes/:?]
//    at java.lang.Thread.run(Thread.java:748) [?:1.8.0_161]

    private static final int NODES = 3;
    private static final int BASE_PORT = 50088;
    private static final int KEY_LENGTH = 1024;
    private static final int VALUE_LENGTH = 1024;
    private List<CoordinatingKeeperServer> servers = new ArrayList<>();

    @Before
    public void before() {
        try {
            FileUtils.deleteDirectory(new File("/Users/gaohaoxiang/export/rocksdb"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<URI> voters = new ArrayList<>();

        for (int i = 0; i < NODES; i++) {
            voters.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        for (int i = 0; i < NODES; i++) {
            Properties properties = new Properties();
            properties.setProperty(KeeperConfigs.STATE_STORE, "rocksdb");
            properties.setProperty("working_dir", String.format("/Users/gaohaoxiang/export/rocksdb/%s", i));

            URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i)));
            CoordinatingKeeperServer server = new CoordinatingKeeperServer(current, voters, RaftServer.Roll.VOTER, properties);
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

//        new Thread(() -> {
//            while (true) {
//                try {
//                    Thread.currentThread().sleep(1000 * 1);
//                } catch (InterruptedException e) {
//                }
//
//                if (RandomUtils.nextInt(0, 100) > 95) {
//                    URI leader = servers.get(0).getLeader();
//                    for (CoordinatingKeeperServer server : servers) {
//                        if (server.getCurrent().equals(leader)) {
//                            System.out.println(String.format("stop leader, uri: %s", leader));
//                            server.stop();
//                        }
//                    }
//                    break;
//                }
//            }
//        }).start();

        while (true) {

            try {
                byte[] key = RandomStringUtils.randomAlphanumeric(KEY_LENGTH).getBytes();
                byte[] value = RandomStringUtils.randomAlphanumeric(VALUE_LENGTH).getBytes();

                long now = System.currentTimeMillis();

                CoordinatingKeeperServer server = servers.get((int) System.currentTimeMillis() % servers.size());

                server.getState().put(key, value);

//                Assert.assertArrayEquals(value, server.getState().get(key));
//
//                server.getState().remove(key);
//
//                Assert.assertEquals(server.getState().get(key), null);

                metrics.mark(System.currentTimeMillis() - now, 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//
//        try {
//            System.in.read();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @After
    public void after() {
        for (CoordinatingKeeperServer server : servers) {
            server.stop();
        }
    }
}