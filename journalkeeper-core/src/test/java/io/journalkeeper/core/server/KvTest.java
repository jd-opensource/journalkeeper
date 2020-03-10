/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.server;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.core.monitor.SimpleMonitorCollector;
import io.journalkeeper.core.serialize.WrappedBootStrap;
import io.journalkeeper.core.serialize.WrappedRaftClient;
import io.journalkeeper.core.state.KvStateFactory;
import io.journalkeeper.monitor.MonitorCollector;
import io.journalkeeper.monitor.MonitoredServer;
import io.journalkeeper.monitor.ServerMonitorInfo;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2019-04-08
 */
public class KvTest {
    private static final Logger logger = LoggerFactory.getLogger(KvTest.class);

    @Test
    public void singleNodeTest() throws Exception {
        setGetTest(1);
    }

    @Test
    public void tripleNodesTest() throws Exception {
        setGetTest(3);
    }

    @Test
    public void fiveNodesTest() throws Exception {
        setGetTest(5);
    }

    @Test
    public void sevenNodesTest() throws Exception {
        setGetTest(7);
    }

    @Test
    public void singleNodeRecoverTest() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("singleNodeTest");
        WrappedBootStrap<String, String, String, String> kvServer = createServers(1, path).get(0);
        WrappedRaftClient<String, String, String, String> kvClient = kvServer.getClient();
        Assert.assertNull(kvClient.update("SET key value").get());
        kvServer.shutdown();

        kvServer = recoverServer("server0", path);
        kvServer.getAdminClient().waitForClusterReady(0L);
        kvClient = kvServer.getClient();

        Assert.assertEquals("value", kvClient.query("GET key").get());
        kvServer.shutdown();
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    @Test
    public void singleNodeAvailabilityTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(1);
    }

    @Test
    public void tripleNodesAvailabilityTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        availabilityTest(3);
    }

    /**
     * 创建N个server，依次停掉每个server，再依次启动，验证集群可用性
     */
    private void availabilityTest(int nodes) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        logger.info("{} nodes availability test.", nodes);
        Path path = TestPathUtils.prepareBaseDir("availabilityTest" + nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            propertiesList.add(properties);
        }
        List<WrappedBootStrap<String, String, String, String>> kvServers = createServers(serverURIs, propertiesList, RaftServer.Roll.VOTER, true);
        int keyNum = 0;
        while (!kvServers.isEmpty()) {
            WrappedRaftClient<String, String, String, String> kvClient = kvServers.get(0).getClient();
            if (kvServers.size() > nodes / 2) {
                kvClient.update("SET key" + keyNum + " value" + keyNum);
            }


            WrappedBootStrap<String, String, String, String> toBeRemoved = kvServers.get(0);

            logger.info("Shutting down server: {}.", toBeRemoved.getServer().serverUri());
            toBeRemoved.shutdown();
            kvServers.remove(toBeRemoved);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                Assert.assertEquals("value" + keyNum, kvServers.get(0).getClient().query("GET key" + keyNum).get());
                keyNum++;
            }
        }

        for (int j = 0; j < nodes; j++) {

            WrappedBootStrap<String, String, String, String> kvServer = recoverServer(propertiesList.get(j));
            kvServers.add(kvServer);
            if (kvServers.size() > nodes / 2) {
                // 等待新的Leader选出来
                logger.info("Wait for new leader...");
                AdminClient adminClient = kvServers.get(0).getAdminClient();
                adminClient.waitForClusterReady(0L);
                for (int i = 0; i < keyNum; i++) {
                    Assert.assertEquals("value" + i, kvServers.get(0).getClient().query("GET key" + i).get());
                }
            }
        }

        stopServers(kvServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }


    private WrappedBootStrap<String, String, String, String> recoverServer(String serverPath, Path path) throws IOException {
        Path workingDir = path.resolve(serverPath);
        Properties properties = new Properties();
        properties.setProperty("working_dir", workingDir.toString());
        properties.setProperty("disable_logo", "true");
        properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
        properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));

        return recoverServer(properties);
    }

    private WrappedBootStrap<String, String, String, String> recoverServer(Properties properties) throws IOException {
        WrappedBootStrap<String, String, String, String> serverBootStrap = new WrappedBootStrap<>(new KvStateFactory(), properties);
        serverBootStrap.getServer().recover();
        serverBootStrap.getServer().start();
        return serverBootStrap;
    }

    private void setGetTest(int nodes) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        Path path = TestPathUtils.prepareBaseDir("SetGetTest-" + nodes);
        List<WrappedBootStrap<String, String, String, String>> kvServers = createServers(nodes, path);
        try {
            List<WrappedRaftClient<String, String, String, String>> kvClients = kvServers.stream()
                    .map(WrappedBootStrap::getClient)
                    .collect(Collectors.toList());


            int i = 0;
            Assert.assertNull(kvClients.get(i++ % kvServers.size()).update("SET key1 hello!").get());
            Assert.assertNull(kvClients.get(i++ % kvServers.size()).update("SET key2 world!").get());
            Assert.assertEquals("hello!", kvClients.get(i++ % kvServers.size()).query("GET key1").get());
            Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                    new HashSet<>(Arrays.stream(kvClients.get(i++ % kvServers.size()).query("KEYS").get().split(",")).map(String::trim).collect(Collectors.toList())));

            Assert.assertNull(kvClients.get(i++ % kvServers.size()).update("DEL key2").get());
            Assert.assertNull(kvClients.get(i++ % kvServers.size()).query("GET key2").get());
            Assert.assertEquals("key1", kvClients.get(i++ % kvServers.size()).query("KEYS").get());
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }

    @Test
    public void localClientTest() throws IOException, ExecutionException, InterruptedException, TimeoutException {

        Path path = TestPathUtils.prepareBaseDir("LocalClientTest");
        List<WrappedBootStrap<String, String, String, String>> kvServers = createServers(1, path);
        try {
            WrappedRaftClient<String, String, String, String> kvClient = kvServers.stream().findFirst().orElseThrow(RuntimeException::new).getLocalClient();


            Assert.assertNull(kvClient.update("SET key1 hello!").get());
            Assert.assertNull(kvClient.update("SET key2 world!").get());
            Assert.assertEquals("hello!", kvClient.query("GET key1").get());
            Assert.assertEquals(new HashSet<>(Arrays.asList("key1", "key2")),
                    new HashSet<>(Arrays.stream(kvClient.query("KEYS").get().split(",")).map(String::trim).collect(Collectors.toList())));

            Assert.assertNull(kvClient.update("DEL key2").get());
            Assert.assertNull(kvClient.query("GET key2").get());
            Assert.assertEquals("key1", kvClient.query("KEYS").get());
        } finally {
            stopServers(kvServers);
            TestPathUtils.destroyBaseDir(path.toFile());
        }
    }


    // 增加节点

    @Test
    public void addVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int newServerCount = 2;
        final int oldServerCount = 3;

        // 初始化并启动一个3个节点的集群
        Path path = TestPathUtils.prepareBaseDir("AddVotersTest-");
        List<WrappedBootStrap<String, String, String, String>> oldServers = createServers(oldServerCount, path);

        WrappedRaftClient<String, String, String, String> kvClient = oldServers.get(0).getClient();

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }

        // 初始化另外2个新节点，先以OBSERVER方式启动
        List<WrappedBootStrap<String, String, String, String>> newServers = new ArrayList<>(newServerCount);
        List<URI> oldConfig = oldServers.stream()
                .map(WrappedBootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(oldServerCount + newServerCount);
        newConfig.addAll(oldConfig);

        logger.info("Create {} observers", newServerCount);
        List<URI> newServerUris = new ArrayList<>(newServerCount);
        for (int i = oldServers.size(); i < oldServers.size() + newServerCount; i++) {
            URI uri = URI.create("local://test" + i);
            newConfig.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            properties.setProperty("observer.parents", String.join(",", oldConfig.stream().map(URI::toString).toArray(String[]::new)));
            WrappedBootStrap<String, String, String, String> kvServer = new WrappedBootStrap<>(RaftServer.Roll.OBSERVER, new KvStateFactory(), properties);
            newServers.add(kvServer);
            newServerUris.add(uri);
        }


        for (int i = 0; i < newServerCount; i++) {
            WrappedBootStrap<String, String, String, String> newServer = newServers.get(i);
            URI uri = newServerUris.get(i);
            newServer.getServer().init(uri, newConfig);
            newServer.getServer().recover();
            newServer.getServer().start();
        }

        AdminClient oldAdminClient = new BootStrap(oldConfig, new Properties()).getAdminClient();
        AdminClient newAdminClient = new BootStrap(newConfig, new Properties()).getAdminClient();

        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        while (null == leaderUri) {
            leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
            Thread.sleep(100L);
        }
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();

        // 等待2个节点拉取数据直到与现有集群同步
        logger.info("Wait for observers to catch up the cluster.");
        boolean synced = false;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= leaderApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }

        // 转换成VOTER
        logger.info("Convert roll to voter of new servers.");

        for (URI uri : newServerUris) {
            newAdminClient.convertRoll(uri, RaftServer.Roll.VOTER).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, newAdminClient.getServerStatus(uri).get().getRoll());
        }

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }
        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
            ServerStatus serverStatus = newAdminClient.getServerStatus(uri).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, serverStatus.getRoll());
            if (leaderUri.equals(uri)) {
                Assert.assertEquals(VoterState.LEADER, serverStatus.getVoterState());
            } else {
                Assert.assertEquals(VoterState.FOLLOWER, serverStatus.getVoterState());
            }
        }


        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), kvClient.query("GET key" + i).get());
        }

        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(newServers);
        stopServers(oldServers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    // 替换节点

    @Test
    public void replaceVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int serverCount = 3;
        final int replaceServerCount = 1;

        // 初始化并启动一个3个节点的集群
        Path path = TestPathUtils.prepareBaseDir("ReplaceVotersTest-");
        List<WrappedBootStrap<String, String, String, String>> oldServers = createServers(serverCount, path);

        WrappedRaftClient<String, String, String, String> kvClient = oldServers.get(0).getClient();

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }


        List<WrappedBootStrap<String, String, String, String>> newServers = new ArrayList<>(replaceServerCount);
        List<URI> oldConfig = oldServers.stream()
                .map(WrappedBootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(serverCount);

        newConfig.addAll(oldConfig);

        newConfig.subList(0, replaceServerCount).clear();


        logger.info("Create {} observers", replaceServerCount);
        List<URI> newServerUris = new ArrayList<>(serverCount);
        for (int i = oldConfig.size(); i < oldConfig.size() + replaceServerCount; i++) {
            URI uri = URI.create("local://test" + i);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("observer.parents", String.join(",", newConfig.stream().map(URI::toString).toArray(String[]::new)));
            properties.setProperty("print_state_interval_sec", String.valueOf(5));

//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            WrappedBootStrap<String, String, String, String> kvServer = new WrappedBootStrap<>(RaftServer.Roll.OBSERVER, new KvStateFactory(), properties);
            newServers.add(kvServer);
            newServerUris.add(uri);
        }
        newConfig.addAll(newServerUris);

        for (int i = 0; i < replaceServerCount; i++) {
            WrappedBootStrap<String, String, String, String> newServer = newServers.get(i);
            URI uri = newServerUris.get(i);
            newServer.getServer().init(uri, newConfig);
            newServer.getServer().recover();
            newServer.getServer().start();
        }


        AdminClient oldAdminClient = new BootStrap(oldConfig, new Properties()).getAdminClient();
        AdminClient newAdminClient = new BootStrap(newConfig, new Properties()).getAdminClient();


        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        while (null == leaderUri) {
            leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
            Thread.sleep(100L);
        }
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();

        // 等待新节点拉取数据直到与现有集群同步
        logger.info("Wait for observers to catch up the cluster.");
        boolean synced = false;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= leaderApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }
        // 转换成VOTER
        logger.info("Convert roll to voter of new servers.");

        for (URI uri : newServerUris) {
            newAdminClient.convertRoll(uri, RaftServer.Roll.VOTER).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, newAdminClient.getServerStatus(uri).get().getRoll());
        }

        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newServerUris.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }


        // 停止已不在集群内的节点

        oldServers.removeIf(server -> {
            if (!newConfig.contains(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                return true;
            } else {
                return false;
            }
        });


        // 可能发生选举，需要等待选举完成。
        newAdminClient.waitForClusterReady();

        WrappedRaftClient<String, String, String, String> newClient = newServers.get(0).getClient();
//        leaderUri = newAdminClient.getClusterConfiguration().get().getLeader();


        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
            ServerStatus serverStatus = newAdminClient.getServerStatus(uri).get();
            Assert.assertEquals(RaftServer.Roll.VOTER, serverStatus.getRoll());
//            if (leaderUri.equals(uri)) {
//                Assert.assertEquals(VoterState.LEADER, serverStatus.getVoterState());
//            } else {
//                Assert.assertNotEquals(VoterState.LEADER, serverStatus.getVoterState());
//            }

        }


        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            logger.info("Query {}...", "key" + i);
            Assert.assertEquals(String.valueOf(i), newClient.query("GET key" + i).get());
        }

        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(newServers);
        stopServers(oldServers);
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    // 减少节点

    @Test
    public void removeVotersTest() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        final int newServerCount = 3;
        final int oldServerCount = 5;

        // 初始化并启动一个5节点的集群
        Path path = TestPathUtils.prepareBaseDir("RemoveVotersTest");
        List<WrappedBootStrap<String, String, String, String>> servers = createServers(oldServerCount, path);

        WrappedRaftClient<String, String, String, String> kvClient = servers.get(0).getClient();

        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }

        List<URI> oldConfig = servers.stream()
                .map(WrappedBootStrap::getServer)
                .map(RaftServer::serverUri)
                .collect(Collectors.toList());
        List<URI> newConfig = new ArrayList<>(newServerCount);
        newConfig.addAll(oldConfig.subList(0, newServerCount));

        AdminClient oldAdminClient = new BootStrap(oldConfig, new Properties()).getAdminClient();
        AdminClient newAdminClient = new BootStrap(newConfig, new Properties()).getAdminClient();
        URI leaderUri = oldAdminClient.getClusterConfiguration().get().getLeader();
        long leaderApplied = oldAdminClient.getServerStatus(leaderUri).get().getLastApplied();
        // 更新集群配置
        logger.info("Update cluster config...");
        boolean success = oldAdminClient.updateVoters(oldConfig, newConfig).get();

        // 验证集群配置
        Assert.assertTrue(success);

        // 等待2阶段变更都提交了
        boolean synced = false;
        long newApplied = leaderApplied + 2;
        while (!synced) {
            synced = newConfig.stream().allMatch(uri -> {
                try {
                    return newAdminClient.getServerStatus(uri).get().getLastApplied() >= newApplied;
                } catch (Throwable e) {
                    return false;
                }
            });
            Thread.sleep(100L);
        }


        // 停止已不在集群内的节点

        servers.removeIf(server -> {
            if (!newConfig.contains(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                return true;
            } else {
                return false;
            }
        });

        // 可能发生选举，需要等待选举完成。
        newAdminClient.waitForClusterReady();

        // 验证所有节点都成功完成了配置变更
        for (URI uri : newConfig) {
            Assert.assertEquals(newConfig, newAdminClient.getClusterConfiguration(uri).get().getVoters());
        }
        WrappedBootStrap<String, String, String, String> clientBootStrap = new WrappedBootStrap<>(newConfig, new Properties());
        kvClient = clientBootStrap.getClient();

        // 读取数据，验证是否正确
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(String.valueOf(i), kvClient.query("GET key" + i).get());
        }
        oldAdminClient.stop();
        newAdminClient.stop();
        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());

    }

    @Test
    public void preferredLeaderTest() throws Exception {
        // 启动5个节点的集群
        int serverCount = 5;
        long timeoutMs = 60000L;
        logger.info("Creating {} nodes cluster...", serverCount);

        Path path = TestPathUtils.prepareBaseDir("PreferredLeaderTest");
        List<WrappedBootStrap<String, String, String, String>> servers = createServers(serverCount, path);
        WrappedRaftClient<String, String, String, String> kvClient = servers.get(0).getClient();

        logger.info("Write some data...");
        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }
        AdminClient adminClient = servers.get(0).getAdminClient();

        // 获取当前leader节点，设为推荐Leader，并停掉这个节点
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        URI preferredLeader = leaderUri;

        Assert.assertNotNull(leaderUri);
        logger.info("Current leader is {}.", leaderUri);
        URI finalLeaderUri = leaderUri;
        Assert.assertTrue(servers.stream().map(WrappedBootStrap::getServer).anyMatch(server -> finalLeaderUri.equals(server.serverUri())));

        Properties properties = null;
        for (WrappedBootStrap<String, String, String, String> server : servers) {
            if (leaderUri.equals(server.getServer().serverUri())) {
                logger.info("Stop server: {}.", server.getServer().serverUri());
                server.shutdown();
                properties = server.getProperties();
                break;
            }
        }

        servers.removeIf(server -> finalLeaderUri.equals(server.getServer().serverUri()));

        logger.info("Wait for new leader...");
        // 等待选出新的leader
        adminClient = servers.get(0).getAdminClient();

        adminClient.waitForClusterReady(0L);
        leaderUri = adminClient.getClusterConfiguration().get().getLeader();
        logger.info("Current leader is {}.", leaderUri);

        // 写入一些数据
        logger.info("Write some data...");
        kvClient = servers.get(0).getClient();
        for (int i = 10; i < 20; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }

        // 启动推荐Leader
        logger.info("Set preferred leader to {}.", preferredLeader);
        adminClient.setPreferredLeader(preferredLeader).get();

        // 重新启动Server
        logger.info("Restart server {}...", preferredLeader);
        WrappedBootStrap<String, String, String, String> recoveredServer = recoverServer(properties);
        servers.add(recoveredServer);
        // 反复检查集群的Leader是否变更为推荐Leader
        logger.info("Checking preferred leader...");
        long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < timeoutMs && !(preferredLeader.equals(adminClient.getClusterConfiguration().get().getLeader()))) {
            Thread.sleep(100L);
        }
        Assert.assertEquals(preferredLeader, adminClient.getClusterConfiguration().get().getLeader());

        // 设置推荐Leader为另一个节点
        URI newPreferredLeader = servers.stream()
                .map(WrappedBootStrap::getServer)
                .map(RaftServer::serverUri)
                .filter(uri -> !preferredLeader.equals(uri))
                .findAny().orElse(null);
        Assert.assertNotNull(newPreferredLeader);

        logger.info("Set preferred leader to {}.", newPreferredLeader);
        adminClient.setPreferredLeader(newPreferredLeader).get();

        // 反复检查集群的Leader是否变更为推荐Leader

        logger.info("Checking preferred leader...");
        t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < timeoutMs && !(newPreferredLeader.equals(adminClient.getClusterConfiguration().get().getLeader()))) {
            Thread.sleep(100L);
        }

        Assert.assertEquals(newPreferredLeader, adminClient.getClusterConfiguration().get().getLeader());


        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    @Test
    public void preVoteTest() throws Exception {
        // 启动5个节点的集群
        int serverCount = 3;
        logger.info("Creating {} nodes cluster...", serverCount);

        Path path = TestPathUtils.prepareBaseDir("preVoteTest");
        List<WrappedBootStrap<String, String, String, String>> servers = createServers(serverCount, path);

        WrappedBootStrap<String, String, String, String> clientBootStrap = new WrappedBootStrap<>(
                servers.stream().map(s -> s.getServer().serverUri()).collect(Collectors.toList()), new Properties()
        );
        WrappedRaftClient<String, String, String, String> kvClient = clientBootStrap.getClient();
        AdminClient adminClient = clientBootStrap.getAdminClient();
        kvClient.waitForClusterReady();

        logger.info("Write some data...");
        // 写入一些数据
        for (int i = 0; i < 10; i++) {
            Assert.assertNull(kvClient.update("SET key" + i + " " + i).get());
        }

        // 获取当前leader节点
        URI leaderUri = adminClient.getClusterConfiguration().get().getLeader();

        Assert.assertNotNull(leaderUri);
        logger.info("Current leader is {}.", leaderUri);

        // 从2个follower中挑选任意一个，停掉
        WrappedBootStrap<String, String, String, String> toBeShutdown = servers.stream().filter(s -> !s.getServer().serverUri().equals(leaderUri)).findAny().orElse(null);
        Assert.assertNotNull(toBeShutdown);
        logger.info("Shutdown server: {}...", toBeShutdown.getServer().serverUri());
        toBeShutdown.shutdown();
        servers.remove(toBeShutdown);
        Properties propertiesOfShutdown = toBeShutdown.getProperties();

        // 记录leader的term，然后停掉leader
        WrappedBootStrap<String, String, String, String> leader = servers.stream().filter(s -> s.getServer().serverUri().equals(leaderUri)).findAny().orElse(null);
        Assert.assertNotNull(leader);
        int term = ((Voter )((Server)leader.getServer()).getServer()).getTerm();
        logger.info("Shutdown leader: {}, term: {}...", leader.getServer().serverUri(), term);
        leader.shutdown();
        servers.remove(leader);
        Properties propertiesOfLeader = leader.getProperties();

        logger.info("Sleep 5 seconds...");
        Thread.sleep(5000L);

        // 重启刚刚2个节点

        logger.info("Restart server {}...", toBeShutdown.getServer().serverUri());
        servers.add(recoverServer(propertiesOfShutdown));

        logger.info("Restart server {}...", leader.getServer().serverUri());
        servers.add(recoverServer(propertiesOfLeader));
        logger.info("Waiting for cluster ready...");
        adminClient.waitForClusterReady();

        // 查看新的Leader上的term
        URI newLeaderUri = adminClient.getClusterConfiguration().get().getLeader();
        leader = servers.stream().filter(s -> s.getServer().serverUri().equals(newLeaderUri)).findAny().orElse(null);
        Assert.assertNotNull(leader);
        int newTerm = ((Voter )((Server)leader.getServer()).getServer()).getTerm();
        // 检查term是否只增加了1
        Assert.assertEquals(term + 1, newTerm);

        stopServers(servers);
        TestPathUtils.destroyBaseDir(path.toFile());
    }

    @Test
    public void monitorTest() throws Exception {

        int nodes = 5;
        Path path = TestPathUtils.prepareBaseDir("monitorTest");
        SimpleMonitorCollector simpleMonitorCollector = ServiceSupport.load(MonitorCollector.class, SimpleMonitorCollector.class);
        Assert.assertNotNull(simpleMonitorCollector);
        for (MonitoredServer monitoredServer : simpleMonitorCollector.getMonitoredServers()) {
            simpleMonitorCollector.removeServer(monitoredServer);
        }

        List<WrappedBootStrap<String, String, String, String>> servers = createServers(nodes, path);
        Collection<MonitoredServer> monitoredServers = simpleMonitorCollector.getMonitoredServers();
        Assert.assertEquals(nodes, monitoredServers.size());
        Collection<ServerMonitorInfo> monitorInfos = simpleMonitorCollector.collectAll();
        Assert.assertEquals(nodes, monitorInfos.size());

        for (ServerMonitorInfo monitorInfo : monitorInfos) {
            logger.info("ServerMonitorInfo: {}.", monitorInfo);
        }

        stopServers(servers);

    }

    private void stopServers(List<WrappedBootStrap<String, String, String, String>> kvServers) {
        for (WrappedBootStrap<String, String, String, String> serverBootStraps : kvServers) {
            try {
                serverBootStraps.shutdown();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", serverBootStraps.getServer().serverUri(), t);
            }
        }
    }

    private List<WrappedBootStrap<String, String, String, String>> createServers(int nodes, Path path) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        return createServers(nodes, path, RaftServer.Roll.VOTER, true);
    }

    private List<WrappedBootStrap<String, String, String, String>> createServers(int nodes, Path path, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");

//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            properties.setProperty("print_state_interval_sec", String.valueOf(5));

            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll, waitForLeader);

    }


    private List<WrappedBootStrap<String, String, String, String>> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        List<WrappedBootStrap<String, String, String, String>> serverBootStraps = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            WrappedBootStrap<String, String, String, String> serverBootStrap = new WrappedBootStrap<>(roll, new KvStateFactory(), propertiesList.get(i));
            serverBootStraps.add(serverBootStrap);

            serverBootStrap.getServer().init(serverURIs.get(i), serverURIs);
            serverBootStrap.getServer().recover();
            serverBootStrap.getServer().start();
        }
        if (waitForLeader) {
            serverBootStraps.get(0).getAdminClient().waitForClusterReady();
        }
        return serverBootStraps;
    }
}
