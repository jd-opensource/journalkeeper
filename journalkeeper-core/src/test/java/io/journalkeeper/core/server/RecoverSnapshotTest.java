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

import io.journalkeeper.core.serialize.WrappedBootStrap;
import io.journalkeeper.core.serialize.WrappedRaftClient;
import io.journalkeeper.core.state.KvStateFactory;
import io.journalkeeper.utils.files.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * RecoverSnapshotTest
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotTest {

    private static final String ROOT = String.format("%s/export/recoverTest", System.getProperty("java.io.tmpdir"));

    @Before
    public void before() throws Exception {
        FileUtils.deleteFolder(new File(ROOT).toPath());
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteFolder(new File(ROOT).toPath());
    }

    @Test
    public void singleTakeAndRecoverTest() throws Exception {
        URI uri = URI.create("local://test");
        File root = new File(ROOT);
        Properties properties = new Properties();
        properties.setProperty("working_dir", root.toString());

        WrappedBootStrap<String, String, String, String> kvServer = new WrappedBootStrap<>(new KvStateFactory(), properties);
        kvServer.getServer().init(uri, Collections.singletonList(uri));
        kvServer.getServer().recover();
        kvServer.getServer().start();
        kvServer.getAdminClient().waitForClusterReady(1000 * 5);

        WrappedRaftClient<String, String, String, String> client = kvServer.getClient();
        Assert.assertNull(client.update("SET key_1 value_1").get());
        Assert.assertEquals("value_1", client.query("GET key_1").get());

        kvServer.getAdminClient().takeSnapshot().get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 2);

        Assert.assertNull(client.update("SET key_2 value_2").get());
        Assert.assertEquals("value_2", client.query("GET key_2").get());

        kvServer.getAdminClient().recoverSnapshot(2).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 3);
        Assert.assertEquals("value_1", client.query("GET key_1").get());
        Assert.assertNull(client.query("GET key_2").get());

        kvServer.getAdminClient().recoverSnapshot(4).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        Assert.assertEquals("value_1", client.query("GET key_1").get());
        Assert.assertEquals("value_2", client.query("GET key_2").get());

        kvServer.getAdminClient().recoverSnapshot(0).get();
        Assert.assertNull(client.query("GET key_1").get());
        Assert.assertNull(client.query("GET key_2").get());

        kvServer.shutdown();
    }

    @Test
    public void clusterTakeAndRecoverTest() throws Exception {
        List<URI> uris = new ArrayList<>();
        List<WrappedBootStrap<String, String, String, String>> servers = new ArrayList<>();
        List<WrappedRaftClient<String, String, String, String>> clients = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("local://test" + i);
            uris.add(uri);
        }

        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("local://test" + i);
            File root = new File(ROOT);
            Properties properties = new Properties();
            properties.setProperty("working_dir", root.toString() + "/" + i);

            WrappedBootStrap<String, String, String, String> kvServer = new WrappedBootStrap<>(new KvStateFactory(), properties);
            kvServer.getServer().init(uri, uris);
            kvServer.getServer().recover();
            kvServer.getServer().start();
            servers.add(kvServer);
            clients.add(kvServer.getClient());
        }
        servers.get(0).getAdminClient().waitForClusterReady(1000 * 5);

        for (WrappedRaftClient<String, String, String, String> client : clients) {
            Assert.assertNull(client.update("SET key_1 value_1").get());
            Assert.assertEquals("value_1", client.query("GET key_1").get());
        }

        servers.get(0).getAdminClient().takeSnapshot().get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 2);
        }

        for (WrappedRaftClient<String, String, String, String> client : clients) {
            Assert.assertNull(client.update("SET key_2 value_2").get());
            Assert.assertEquals("value_2", client.query("GET key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(4).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 3);
        }

        for (WrappedRaftClient<String, String, String, String> client : clients) {
            Assert.assertEquals("value_1", client.query("GET key_1").get());
            Assert.assertNull(client.query("GET key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(8).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        }

        for (WrappedRaftClient<String, String, String, String> client : clients) {
            Assert.assertEquals("value_1", client.query("GET key_1").get());
            Assert.assertEquals("value_2", client.query("GET key_2").get());
        }

        servers.get(0).getAdminClient().recoverSnapshot(0).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 5);
        }

        for (WrappedRaftClient<String, String, String, String> client : clients) {
            Assert.assertNull(client.query("GET key_1").get());
            Assert.assertNull(client.query("GET key_2").get());
        }

        for (WrappedBootStrap<String, String, String, String> server : servers) {
            server.shutdown();
        }

    }
}