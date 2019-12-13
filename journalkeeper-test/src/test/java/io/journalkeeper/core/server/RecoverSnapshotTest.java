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
package io.journalkeeper.core.server;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.examples.kv.KvClient;
import io.journalkeeper.examples.kv.KvServer;
import io.journalkeeper.utils.files.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
        URI uri = URI.create("jk://localhost:50088");
        File root = new File(ROOT);
        Properties properties = new Properties();
        properties.setProperty("working_dir", root.toString());

        KvServer kvServer = new KvServer(RaftServer.Roll.VOTER, properties);
        kvServer.init(uri, Arrays.asList(uri));
        kvServer.recover();
        kvServer.start();
        kvServer.getAdminClient().waitForClusterReady(1000 * 5);

        KvClient client = kvServer.createClient();
        client.set("key_1", "value_1");
        Assert.assertEquals(client.get("key_1"), "value_1");

        kvServer.getAdminClient().takeSnapshot().get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 2);

        client.set("key_2", "value_2");
        Assert.assertEquals(client.get("key_2"), "value_2");

        kvServer.getAdminClient().recoverSnapshot(2).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 3);
        Assert.assertEquals(client.get("key_1"), "value_1");
        Assert.assertEquals(client.get("key_2"), null);

        kvServer.getAdminClient().recoverSnapshot(4).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        Assert.assertEquals(client.get("key_1"), "value_1");
        Assert.assertEquals(client.get("key_2"), "value_2");

        kvServer.getAdminClient().recoverSnapshot(0).get();
        Assert.assertEquals(kvServer.getAdminClient().getSnapshots().get().getSnapshots().size(), 5);
        Assert.assertEquals(client.get("key_1"), null);
        Assert.assertEquals(client.get("key_2"), null);

        kvServer.stop();
    }

    @Test
    public void clusterTakeAndRecoverTest() throws Exception {
        List<URI> uris = new ArrayList<>();
        List<KvServer> servers = new ArrayList<>();
        List<KvClient> clients = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("jk://localhost:" + (50088 + i));
            uris.add(uri);
        }

        for (int i = 0; i < 3; i++) {
            URI uri = URI.create("jk://localhost:" + (50088 + i));
            File root = new File(ROOT);
            Properties properties = new Properties();
            properties.setProperty("working_dir", root.toString() + "/" + i);

            KvServer server = new KvServer(RaftServer.Roll.VOTER, properties);
            server.init(uri, uris);
            server.recover();
            server.start();
            servers.add(server);
            clients.add(server.createClient());
        }
        servers.get(0).getAdminClient().waitForClusterReady(1000 * 5);

        for (KvClient client : clients) {
            client.set("key_1", "value_1");
            Assert.assertEquals(client.get("key_1"), "value_1");
        }

        servers.get(0).getAdminClient().takeSnapshot().get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 2);
        }

        for (KvClient client : clients) {
            client.set("key_2", "value_2");
            Assert.assertEquals(client.get("key_2"), "value_2");
        }

        servers.get(0).getAdminClient().recoverSnapshot(4).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 3);
        }

        for (KvClient client : clients) {
            Assert.assertEquals(client.get("key_1"), "value_1");
            Assert.assertEquals(client.get("key_2"), null);
        }

        servers.get(0).getAdminClient().recoverSnapshot(8).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 4);
        }

        for (KvClient client : clients) {
            Assert.assertEquals(client.get("key_1"), "value_1");
            Assert.assertEquals(client.get("key_2"), "value_2");
        }

        servers.get(0).getAdminClient().recoverSnapshot(0).get();

        for (int i = 0; i < servers.size(); i++) {
            Assert.assertEquals(servers.get(i).getAdminClient().getSnapshots().get().getSnapshots().size(), 5);
        }

        for (KvClient client : clients) {
            Assert.assertEquals(client.get("key_1"), null);
            Assert.assertEquals(client.get("key_2"), null);
        }

        for (KvServer server : servers) {
            server.stop();
        }

    }
}