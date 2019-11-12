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
package io.journalkeeper.sql.test.h2;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.server.AbstractServer;
import io.journalkeeper.sql.client.BatchSQLOperator;
import io.journalkeeper.sql.client.SQLClient;
import io.journalkeeper.sql.client.SQLClientAccessPoint;
import io.journalkeeper.sql.client.SQLOperator;
import io.journalkeeper.sql.client.support.DefaultSQLOperator;
import io.journalkeeper.sql.druid.config.DruidConfigs;
import io.journalkeeper.sql.server.SQLServer;
import io.journalkeeper.sql.server.SQLServerAccessPoint;
import io.journalkeeper.sql.state.config.SQLConfigs;
import io.journalkeeper.sql.state.jdbc.config.JDBCConfigs;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
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
 * date: 2019/8/7
 */
public class H2Test {

    private static final int NODES = 3;
    private static final int BASE_PORT = 50088;
    private List<SQLServer> servers = new ArrayList<>();
    private List<SQLClient> clients = new ArrayList<>();

    @Before
    public void before() {
        try {
            FileUtils.deleteDirectory(new File(String.format("%s/export/h2", System.getProperty("user.dir"))));
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<URI> servers = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            servers.add(URI.create(String.format("journalkeeper://127.0.0.1:%s", (BASE_PORT + i))));
        }

        for (int i = 0; i < NODES; i++) {
            Properties properties = new Properties();
            properties.setProperty(AbstractServer.Config.WORKING_DIR_KEY, String.format("%s/export/h2/%s", System.getProperty("user.dir"), i));
            properties.setProperty(AbstractServer.Config.RPC_TIMEOUT_MS_KEY, String.valueOf(1000 * 10));
//            properties.setProperty(SQLConfigs.TRANSACTION_CLEAR_INTERVAL, String.valueOf(1));
//            properties.setProperty(SQLConfigs.TRANSACTION_TIMEOUT, String.valueOf(1000 * 1));
            properties.setProperty(SQLConfigs.INIT_FILE, "/topic.sql");
            properties.setProperty(JDBCConfigs.DATASOURCE_TYPE, "druid");
            properties.setProperty(DruidConfigs.URL, "jdbc:h2:file:{datasource.path}/joyqueue;DB_CLOSE_DELAY=TRUE;AUTO_SERVER=TRUE");
            properties.setProperty(DruidConfigs.DRIVER_CLASS, "org.h2.Driver");

            URI current = URI.create(String.format("journalkeeper://127.0.0.1:%s", BASE_PORT + i));
            SQLServerAccessPoint serverAccessPoint = new SQLServerAccessPoint(properties);
            SQLServer server = serverAccessPoint.createServer(current, servers, RaftServer.Roll.VOTER);
            server.start();

            this.servers.add(server);
        }

        SQLClientAccessPoint clientAccessPoint = new SQLClientAccessPoint(new Properties());

        for (int i = 0; i < NODES; i++) {
            SQLClient client = clientAccessPoint.createClient(servers);
            this.clients.add(client);
        }

        this.servers.get(0).waitClusterReady(1000 * 30, TimeUnit.MILLISECONDS);
        try {
            Thread.currentThread().sleep(1000 * 5);
        } catch (InterruptedException e) {
        }
    }

    @Ignore
    @Test
    public void test() throws Exception {
        for (int i = 0; i < 1; i++) {
            int index = i;
            new Thread(() -> {
                while (true) {
                    try {
                        doTest(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        System.in.read();
    }

    protected void doTest(int index) throws Exception {
        SQLOperator sqlOperator = new DefaultSQLOperator(clients.get(index % NODES));
        System.out.println(sqlOperator.query("SELECT NOW()"));
        System.out.println(sqlOperator.query("SHOW TABLES"));

        System.out.println(sqlOperator.query("SELECT * FROM topic"));
        System.out.println(sqlOperator.query("SELECT * FROM topic WHERE code = 'test'"));
//
        new Thread(() -> {
            while (true) {
                System.out.println(sqlOperator.query("SELECT * FROM topic WHERE code = 'code_value'"));
                try {
                    Thread.currentThread().sleep(1000 * 1);
                } catch (InterruptedException e) {
                }
            }
        }).start();

        sqlOperator.insert("INSERT INTO topic(id, code, namespace, partitions, priority_partitions, type) " +
                "VALUES(?,?,?,?,?,?)", "id_value_" + System.nanoTime(), "code_value", "namespace_value", 1 * index, 0, 0);

        BatchSQLOperator batchOperator = sqlOperator.beginBatch();

        batchOperator.insert("INSERT INTO topic(id, code, namespace, partitions, priority_partitions, type) " +
                "VALUES(?,?,?,?,?,?)", "id_value_" + System.nanoTime(), "code_value", "namespace_value", 2 * index, 0, 0);
        batchOperator.insert("INSERT INTO topic(id, code, namespace, partitions, priority_partitions, type) " +
                "VALUES(?,?,?,?,?,?)", "id_value_" + System.nanoTime(), "code_value", "namespace_value", 3 * index, 0, 0);
        Thread.currentThread().sleep(1000 * 5);
        batchOperator.commit();
        System.out.println("commit batch");
        Thread.currentThread().sleep(1000 * 5);
    }
}