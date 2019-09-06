/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.examples.kv.KvClient;
import io.journalkeeper.examples.kv.KvServer;
import io.journalkeeper.utils.net.NetworkingUtils;
import io.journalkeeper.utils.state.StateServer;
import io.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-04-08
 */
public class KvTest {
    private static final Logger logger = LoggerFactory.getLogger(KvTest.class);
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
    public void singleNodeRecoverTest() throws Exception{
        Path path = TestPathUtils.prepareBaseDir("singleNodeTest");
        KvServer kvServer = createServers(1, path).get(0);
        KvClient kvClient = kvServer.createClient();
        kvClient.set("key", "value");
        kvServer.stop();

        kvServer = recoverServer("server0", path);

        kvClient = kvServer.createClient();
        kvClient.waitForLeader(10000);
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
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            propertiesList.add(properties);
        }
        List<KvServer> kvServers = createServers(serverURIs, propertiesList, RaftServer.Roll.VOTER,true);
        int keyNum = 0;
        while (!kvServers.isEmpty()) {
            KvClient kvClient = kvServers.get(0).createClient();
            if (kvServers.size() > nodes / 2) {
                long t0 = System.currentTimeMillis();
                kvClient.set("key" + keyNum, "value" + keyNum);
            }


            KvServer toBeRemoved = kvServers.get(0);

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
        properties.setProperty("working_dir", workingDir.toString());
        properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
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


    // 增加节点

    @Test
    public void addVotersTest() throws IOException, InterruptedException, ExecutionException {
        final int newServerCount = 2;
        final int oldServerCount = 3;

        // 初始化并启动一个3个节点的集群
        Path path = TestPathUtils.prepareBaseDir("AddVotersTest");
        List<KvServer> oldServers = createServers(oldServerCount, path);

        KvClient kvClient = oldServers.get(0).createClient();

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            kvClient.set("key" + i, String.valueOf(i));
        }

        // 初始化另外2个新节点，先以OBSERVER方式启动
        List<KvServer> newServers = new ArrayList<>(newServerCount);
        List<URI> oldConfig = oldServers.stream().map(KvServer::serverUri).collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(oldServerCount + newServerCount);
        newConfig.addAll(oldConfig);

        logger.info("Create {} observers", newServerCount);
        List<URI> newServerUris = new ArrayList<>(newServerCount);
        for (int i = oldServers.size(); i < oldServers.size() + newServerCount; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            newConfig.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("enable_metric", "true");
            properties.setProperty("print_metric_interval_sec", "3");
            KvServer kvServer = new KvServer(RaftServer.Roll.OBSERVER, properties);
            kvServer.getParents().addAll(oldConfig);
            newServers.add(kvServer);
            newServerUris.add(uri);
        }


        for (int i = 0; i < newServerCount; i++) {
            KvServer newServer = newServers.get(i);
            URI uri = newServerUris.get(i);
            newServer.init(uri, newConfig);
            newServer.recover();
            newServer.start();
        }

        URI leaderUri = kvClient.leaderUri();
        while (null == leaderUri) {
            leaderUri = kvClient.leaderUri();
            Thread.sleep(100L);
        }
        long leaderApplied = kvClient.serverStatus(leaderUri).getLastApplied();

        // 等待2个节点拉取数据直到与现有集群同步
        logger.info("Wait for observers to catch up the cluster.");
        boolean synced = false;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> kvClient.serverStatus(uri).getLastApplied() == leaderApplied);
            Thread.sleep(100L);
        }

        // 转换成VOTER
        logger.info("Convert roll to voter of new servers.");

        for (URI uri : newServerUris) {
            kvClient.convertRoll(uri, RaftServer.Roll.VOTER);
            Assert.assertEquals(RaftServer.Roll.VOTER, kvClient.serverStatus(uri).getRoll());
        }

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = kvClient.updateVoters(oldConfig, newConfig);

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        synced = false;
        long newApplied = leaderApplied + 2;
        while (synced) {
            synced = newConfig.stream().allMatch(uri -> (kvClient.serverStatus(uri).getLastApplied() == newApplied));
            Thread.sleep(100L);
        }
        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, kvClient.getVoters(uri));
            ServerStatus serverStatus = kvClient.serverStatus(uri);
            Assert.assertEquals(RaftServer.Roll.VOTER, serverStatus.getRoll());
            if (leaderUri.equals(uri)) {
                Assert.assertEquals(VoterState.LEADER, serverStatus.getVoterState());
            } else {
                Assert.assertEquals(VoterState.FOLLOWER, serverStatus.getVoterState());
            }
        }


        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), kvClient.get("key" + i));
        }

    }

    // 减少节点

    @Test
    public void removeVotersTest() throws IOException, InterruptedException, ExecutionException {
        final int newServerCount = 3;
        final int oldServerCount = 5;

        // 初始化并启动一个5节点的集群
        Path path = TestPathUtils.prepareBaseDir("RemoveVotersTest");
        List<KvServer> oldServers = createServers(oldServerCount, path);

        KvClient kvClient = oldServers.get(0).createClient();

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            kvClient.set("key" + i, String.valueOf(i));
        }

        long leaderApplied = kvClient.serverStatus(kvClient.leaderUri()).getLastApplied();
        List<URI> oldConfig = oldServers.stream().map(KvServer::serverUri).collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(newServerCount);
        newConfig.addAll(oldConfig.subList(0, newServerCount));

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = kvClient.updateVoters(oldConfig, newConfig);

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        boolean synced = false;
        long newApplied = leaderApplied + 2;
        while (synced) {
            synced = newConfig.stream().allMatch(uri -> (kvClient.serverStatus(uri).getLastApplied() == newApplied));
            Thread.sleep(100L);
        }

        // 可能发生选举，需要等待选举完成。
        boolean leaderExists = kvClient.waitForLeader(5000L);

        Assert.assertTrue(leaderExists);

        // 停止已不在集群内的节点

        for (KvServer server : oldServers) {
            if(!newConfig.contains(server.serverUri())) {
                server.stop();
            }
        }
        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
                Assert.assertEquals(newConfig, kvClient.getVoters(uri));
        }

        KvClient newClient = oldServers.get(0).createClient();

        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), newClient.get("key" + i));
        }

    }


    // 增加并且减少




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
        return createServers(nodes, path, RaftServer.Roll.VOTER, true);
    }

    private List<KvServer> createServers(int nodes, Path path, RaftServer.Roll roll, boolean waitForLeader) throws IOException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll,waitForLeader);

    }



    private List<KvServer> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) throws IOException {

        List<KvServer> kvServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            KvServer kvServer = new KvServer(roll, propertiesList.get(i));
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



}
