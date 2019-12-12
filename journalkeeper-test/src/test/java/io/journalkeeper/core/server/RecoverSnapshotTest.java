package io.journalkeeper.core.server;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.examples.kv.KvClient;
import io.journalkeeper.examples.kv.KvServer;
import io.journalkeeper.utils.files.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

/**
 * RecoverSnapshotTest
 * author: gaohaoxiang
 * date: 2019/12/12
 */
public class RecoverSnapshotTest {

    private static final String ROOT = String.format("%s/export/recoverTest", System.getProperty("user.dir"));

    @Before
    public void before() throws Exception {
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

        client.set("key_2", "value_2");
        Assert.assertEquals(client.get("key_2"), "value_2");

        kvServer.getAdminClient().recoverSnapshot(2).get();
        Assert.assertEquals(client.get("key_1"), "value_1");
        Assert.assertEquals(client.get("key_2"), null);

        kvServer.getAdminClient().recoverSnapshot(4).get();
        Assert.assertEquals(client.get("key_1"), "value_1");
        Assert.assertEquals(client.get("key_2"), "value_2");

        kvServer.getAdminClient().recoverSnapshot(0).get();
        Assert.assertEquals(client.get("key_1"), null);
        Assert.assertEquals(client.get("key_2"), null);
    }

    @Test
    public void clusterTakeAndRecoverTest() {

    }
}