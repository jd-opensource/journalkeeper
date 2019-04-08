package com.jd.journalkeeper.core;

import com.jd.journalkeeper.core.exception.NoLeaderException;
import com.jd.journalkeeper.examples.kv.KvClient;
import com.jd.journalkeeper.examples.kv.KvServer;
import com.jd.journalkeeper.utils.net.NetworkingUtils;
import com.jd.journalkeeper.utils.test.TestPathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
    private Path path = null;

    @Test
    public void singleNodeTest() throws IOException {
        List<KvServer> kvServers = createServers(1);
        setGetTest(kvServers);
        stopServers(kvServers);
    }

    @Test
    public void tripleNodesTest() throws IOException {
        List<KvServer> kvServers = createServers(3);
        setGetTest(kvServers);
        stopServers(kvServers);
    }

    @Test
    public void fiveNodesTest() throws IOException {
        List<KvServer> kvServers = createServers(5);
        setGetTest(kvServers);
        stopServers(kvServers);
    }
    @Test
    public void sevenNodesTest() throws IOException {
        List<KvServer> kvServers = createServers(7);
        setGetTest(kvServers);
        stopServers(kvServers);
    }

    @Test
    public void singleNodeRecoverTest() throws IOException {
        KvServer kvServer = createServers(1).get(0);
        KvClient kvClient = kvServer.createClient();
        kvClient.set("key", "value");
        while (!kvServer.flush()) {
            Thread.yield();
        }
        kvServer.stop();

        kvServer = recoverServer("server0");
        kvServer.waitForLeaderReady();
        kvClient = kvServer.createClient();
        Assert.assertEquals("value", kvClient.get("key"));
        kvServer.stop();

    }

    private void availabilityTest(int nodes) throws IOException {
        logger.info("Nodes: {}", nodes);
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
        int i = 0;
        while (!kvServers.isEmpty()) {
            KvClient kvClient = kvServers.get(0).createClient();
            kvClient.set("key" + i, "value" + i);
            KvServer leader = kvServers.stream()
                    .filter(kvServer -> kvServer.serverUri() == kvClient.getClusterConfiguration().getLeader())
                    .findAny().orElse(null);
            Assert.assertNotNull(leader);
            leader.stop();
            kvServers.remove(leader);
            //TODO: 小于半数的情况下，选不出Leader
            kvServers.get(0).waitForLeaderReady();
            Assert.assertEquals("value" + i, kvServers.get(0).createClient().get("key" + i));
            i ++;
        }

    }


    private KvServer recoverServer(String serverPath) throws IOException {
        KvServer kvServer;
        Path workingDir = path.resolve(serverPath);
        Properties properties = new Properties();
        properties.put("working_dir", workingDir.toString());
        kvServer = new KvServer(properties);
        kvServer.recover();
        kvServer.start();
        return kvServer;
    }

    private void setGetTest(List<KvServer> kvServers) {
        List<KvClient> kvClients = kvServers.stream().map(KvServer::createClient).collect(Collectors.toList());


        int i = 0;
        kvClients.get(i++ % kvServers.size()).set("key1", "hello!");
        kvClients.get(i++ % kvServers.size()).set("key2", "world!");
        Assert.assertEquals("hello!", kvClients.get(i++ % kvServers.size()).get("key1"));
        Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                new HashSet<>(kvClients.get(i++ % kvServers.size()).listKeys()));

        kvClients.get(i++ % kvServers.size()).del("key2");
        Assert.assertNull(kvClients.get(i++ % kvServers.size()).get("key2"));
        Assert.assertEquals(Collections.singletonList("key1"),kvClients.get(i++ % kvServers.size()).listKeys());
    }



    @Before
    public void before() throws IOException {
        path = TestPathUtils.prepareBaseDir();

    }

    private void stopServers(List<KvServer> kvServers) {
        try {
            kvServers.parallelStream().forEach(KvServer::stop);
        } catch (Throwable ignored) {}
    }

    private List<KvServer> createServers(int nodes) throws IOException {
        return createServers(nodes, true);
    }
    private List<KvServer> createServers(int nodes, boolean waitForLeader) throws IOException {
        logger.info("Nodes: {}", nodes);
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
    @After
    public void after() {
        TestPathUtils.destroyBaseDir();

    }
}
