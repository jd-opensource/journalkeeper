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
package io.journalkeeper.coordinating;

import io.journalkeeper.coordinating.client.CoordinatingClient;
import io.journalkeeper.coordinating.client.CoordinatingClientAccessPoint;
import io.journalkeeper.coordinating.server.CoordinatingServer;
import io.journalkeeper.coordinating.server.CoordinatingServerAccessPoint;
import io.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.utils.test.TestPathUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatingServerTest {


    private static final int NODES = 3;
    private static final int BASE_PORT = 50088;
    private static final int KEY_LENGTH = 1024;
    private static final int VALUE_LENGTH = 1024;
    private List<CoordinatingServer> servers = new ArrayList<>();
    private List<CoordinatingClient> clients = new ArrayList<>();
    private static final String WORKING_DIR = "CoordinatingServerTest";
    private Path base = null;
    @Before
    public void before() throws IOException, ExecutionException, InterruptedException, TimeoutException {

        base = TestPathUtils.prepareBaseDir(WORKING_DIR );
        List<URI> voters = new ArrayList<>();

        for (int i = 0; i < NODES; i++) {
            voters.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        for (int i = 0; i < NODES; i++) {
            Properties properties = new Properties();
            properties.setProperty(CoordinatingConfigs.STATE_STORE, "rocksdb");
            properties.setProperty("working_dir", base.resolve("rocksdb" + i).toString());

            properties.setProperty("rocksdb.options.createIfMissing", "true");
            properties.setProperty("rocksdb.options.writeBufferSize", "134217728");
            properties.setProperty("rocksdb.options.minWriteBufferNumberToMerge", "2");
            properties.setProperty("rocksdb.options.level0FileNumCompactionTrigger", "10");
            properties.setProperty("rocksdb.options.targetFileSizeBase", "268435456");
            properties.setProperty("rocksdb.options.maxBytesForLevelBase", "2684354560");
            properties.setProperty("rocksdb.options.targetFileSizeMultiplier", "10");
            properties.setProperty("rocksdb.options.maxBackgroundCompactions", "8");
            properties.setProperty("rocksdb.options.maxBackgroundFlushes", "1");
            properties.setProperty("rocksdb.options.skipStatsUpdateOnDbOpen", "true");
            properties.setProperty("rocksdb.options.optimizeFiltersForHits", "true");
            properties.setProperty("rocksdb.options.newTableReaderForCompactionInputs", "true");

            properties.setProperty("rocksdb.table.options.blockSize", "262144");
            properties.setProperty("rocksdb.table.options.cacheIndexAndFilterBlocks", "true");
            properties.setProperty("rocksdb.filter.bitsPerKey", "10");

            URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i)));
            CoordinatingServerAccessPoint coordinatingServerAccessPoint = new CoordinatingServerAccessPoint(properties);
            CoordinatingServer server = coordinatingServerAccessPoint.createServer(current, voters, RaftServer.Roll.VOTER);
            server.start();
            servers.add(server);
        }

        CoordinatingClientAccessPoint coordinatingClientAccessPoint = new CoordinatingClientAccessPoint(new Properties());

        for (int i = 0; i < NODES; i++) {
            CoordinatingClient client = coordinatingClientAccessPoint.createClient(voters);
            clients.add(client);
        }

        clients.get(0).waitClusterReady(0L);
    }

    @Ignore
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
//                    for (CoordinatingServer server : servers) {
//                        if (server.getCurrent().equals(leader)) {
//                            System.out.println(String.format("stop leader, uri: %s", leader));
//                            server.stop();
//                        }
//                    }
//                    break;
//                }
//            }
//        }).start();

//        clients.get(0).watch(new CoordinatingEventListener() {
//            @Override
//            public void onEvent(CoordinatingEvent event) {
//                System.out.println(String.format("type: %s, key: %s", event.getType(), new String(event.getKey())));
//            }
//        });
//
//        clients.get(0).watch(key, new CoordinatingEventListener() {
//            @Override
//            public void onEvent(CoordinatingEvent event) {
//                System.out.println(String.format("type: %s, key: %s", event.getType(), new String(event.getKey())));
//            }
//        });

        while (true) {

            try {
                byte[] key = RandomStringUtils.randomAlphanumeric(KEY_LENGTH).getBytes();
                byte[] value = RandomStringUtils.randomAlphanumeric(VALUE_LENGTH).getBytes();

                long now = System.currentTimeMillis();

                clients.get((int) System.currentTimeMillis() % clients.size()).set(key, value).get();

//                servers.get((int) System.currentTimeMillis() % servers.size()).getClient().set(key, value);

//                Assert.assertArrayEquals(value, clients.get(0).get(key));
//                clients.get(0).remove(key);
//                Assert.assertEquals(clients.get(0).get(key), null);
//
//                clients.get(0).list(Arrays.asList(key));

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
        for (CoordinatingClient client : clients) {
            client.stop();
        }
        for (CoordinatingServer server : servers) {
            server.stop();
        }

        TestPathUtils.destroyBaseDir(base.toFile());
    }
}