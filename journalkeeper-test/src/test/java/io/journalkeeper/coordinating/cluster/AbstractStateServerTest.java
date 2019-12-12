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
package io.journalkeeper.coordinating.cluster;

import io.journalkeeper.coordinating.server.CoordinatingServer;
import io.journalkeeper.coordinating.server.CoordinatingServerAccessPoint;
import io.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import io.journalkeeper.core.api.RaftServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * AbstractStateServerTest
 * author: gaohaoxiang
 *
 * date: 2019/6/3
 */
public abstract class AbstractStateServerTest {

    private static final int NODES = 5;
    private static final int BASE_PORT = 50088;
    private CoordinatingServer server;

    @Before
    public void before() {
        List<URI> voters = new ArrayList<>();

        for (int i = 0; i < NODES; i++) {
            voters.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        Properties properties = new Properties();
        properties.setProperty(CoordinatingConfigs.STATE_STORE, "rocksdb");
        properties.setProperty("working_dir", String.format("/Users/gaohaoxiang/export/rocksdb/%s", getIndex()));

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

        URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + getIndex())));
        CoordinatingServerAccessPoint coordinatingServerAccessPoint = new CoordinatingServerAccessPoint(properties);
        CoordinatingServer server = coordinatingServerAccessPoint.createServer(current, voters, RaftServer.Roll.VOTER);
        server.start();

        this.server = server;
    }

    @Test
    public void test() {
        try {
            System.in.read();
        } catch (IOException e) {
        }
    }

    protected abstract int getIndex();
}