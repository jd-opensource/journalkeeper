package com.jd.journalkeeper.core;

import com.jd.journalkeeper.examples.kv.KvClient;
import com.jd.journalkeeper.examples.kv.KvServer;
import com.jd.journalkeeper.utils.net.NetworkingUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author liyue25
 * Date: 2019-04-08
 */
public class KvTest {
    private static final Logger logger = LoggerFactory.getLogger(KvTest.class);
    // TODO: OOM 问题
    @Test
    public void singleNodeTest() throws IOException {
        setGetTest(1);
    }

    @Test
    public void tripleNodesTest() throws IOException {
        setGetTest(3);
    }

    @Test
    public void fiveNodesTest() throws IOException {
        setGetTest(5);
    }
    @Test
    public void sevenNodesTest() throws IOException {
        setGetTest(7);
    }

    @Test
    public void singleNodeRecoverTest() throws IOException {
        Path path = TestPathUtils.prepareBaseDir("singleNodeTest");
        KvServer kvServer = createServers(1, path).get(0);
        KvClient kvClient = kvServer.createClient();
        kvClient.set("key", "value");
        while (!kvServer.flush()) {
            Thread.yield();
        }
        kvServer.stop();

        kvServer = recoverServer("server0", path);
        kvServer.waitForLeaderReady();
        kvClient = kvServer.createClient();
        Assert.assertEquals("value", kvClient.get("key"));
        kvServer.stop();
        TestPathUtils.destroyBaseDir(path.toFile());

    }
    @Test
    public void singleNodeAvailabilityTest() throws IOException, InterruptedException {
        availabilityTest(1);
    }

    @Test
    public void tripleNodesAvailabilityTest() throws IOException, InterruptedException {
        availabilityTest(3);
    }

    /**
     * 创建N个server，依次停掉每个server，再依次启动，验证集群可用性
     */
    private void availabilityTest(int nodes) throws IOException, InterruptedException {
        logger.info("{} nodes availability test.", nodes);
        Path path = TestPathUtils.prepareBaseDir("availabilityTest" + nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.put("working_dir", workingDir.toString());
            propertiesList.add(properties);
        }
        List<KvServer> kvServers = createServers(serverURIs, propertiesList,true);
        int keyNum = 0;
        while (!kvServers.isEmpty()) {
            KvClient kvClient = kvServers.get(0).createClient();
            if (kvServers.size() > nodes / 2) {
                long t0 = System.currentTimeMillis();
                kvClient.set("key" + keyNum, "value" + keyNum);
            }


            KvServer toBeRemoved = kvServers.get(0);

            while (!toBeRemoved.flush()) {
                Thread.yield();
            }
            logger.info("Shutting down server: {}.", toBeRemoved.serverUri());
            toBeRemoved.stop();
            kvServers.remove(toBeRemoved);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                Thread.sleep(5000L);
                Assert.assertEquals("value" + keyNum, kvServers.get(0).createClient().get("key" + keyNum));
                keyNum++;
            }
        }

        for (int j = 0; j < nodes; j++) {

            KvServer kvServer = recoverServer(propertiesList.get(j));
            kvServers.add(kvServer);
            if(kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                Thread.sleep(5000L);
                for (int i = 0; i < keyNum; i++) {
                    Assert.assertEquals("value" + i, kvServers.get(0).createClient().get("key" + i));
                }
            }
        }

        stopServers(kvServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }


    private KvServer recoverServer(String serverPath, Path path) throws IOException {
        KvServer kvServer;
        Path workingDir = path.resolve(serverPath);
        Properties properties = new Properties();
        properties.put("working_dir", workingDir.toString());
        kvServer = new KvServer(properties);
        kvServer.recover();
        kvServer.start();
        return kvServer;
    }

    private KvServer recoverServer(Properties properties) throws IOException {
        KvServer kvServer;
        kvServer = new KvServer(properties);
        kvServer.recover();
        kvServer.start();
        return kvServer;
    }

    private void setGetTest(int nodes) throws IOException {

        Path path = TestPathUtils.prepareBaseDir("SetGetTest-" + nodes);
        List<KvServer> kvServers = createServers(nodes, path);
        try {
            List<KvClient> kvClients = kvServers.stream().map(KvServer::createClient).collect(Collectors.toList());


            int i = 0;
            kvClients.get(i++ % kvServers.size()).set("key1", "hello!");
            kvClients.get(i++ % kvServers.size()).set("key2", "world!");
            Assert.assertEquals("hello!", kvClients.get(i++ % kvServers.size()).get("key1"));
            Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                    new HashSet<>(kvClients.get(i++ % kvServers.size()).listKeys()));

            kvClients.get(i++ % kvServers.size()).del("key2");
            Assert.assertNull(kvClients.get(i++ % kvServers.size()).get("key2"));
            Assert.assertEquals(Collections.singletonList("key1"), kvClients.get(i++ % kvServers.size()).listKeys());
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }

    private void stopServers(List<KvServer> kvServers) {
        for(KvServer kvServer: kvServers) {
            try {
                kvServer.stop();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", kvServer.serverUri(), t);
            }
        }
    }

    private List<KvServer> createServers(int nodes, Path path) throws IOException {
        return createServers(nodes, path ,true);
    }
    private List<KvServer> createServers(int nodes, Path path, boolean waitForLeader) throws IOException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.put("working_dir", workingDir.toString());
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList,waitForLeader);

    }
    private List<KvServer> createServers(List<URI> serverURIs, List<Properties> propertiesList, boolean waitForLeader) throws IOException {

        List<KvServer> kvServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            KvServer kvServer = new KvServer(propertiesList.get(i));
            kvServers.add(kvServer);
            kvServer.init(serverURIs.get(i), serverURIs);
            kvServer.recover();
            kvServer.start();
        }
        if(waitForLeader) {
            kvServers.get(0).waitForLeaderReady();
        }
        return kvServers;
    }
    private List<KvServer> restoreServers(List<Properties> propertiesList, boolean waitForLeader) throws IOException {

        List<KvServer> kvServers = new ArrayList<>(propertiesList.size());
        for (Properties properties : propertiesList) {
            KvServer kvServer = new KvServer(properties);
            kvServers.add(kvServer);
            kvServer.recover();
            kvServer.start();
        }
        if(waitForLeader) {
            kvServers.get(0).waitForLeaderReady();
        }
        return kvServers;
    }

}
