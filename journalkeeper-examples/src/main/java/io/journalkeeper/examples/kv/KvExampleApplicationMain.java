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
package io.journalkeeper.examples.kv;

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.utils.net.NetworkingUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KvExampleApplicationMain {
    private static final Logger logger = LoggerFactory.getLogger(KvExampleApplicationMain.class);
    public static void main(String [] args) throws IOException, ExecutionException, InterruptedException {
        int nodes = 1;
        logger.info("Usage: java " + KvExampleApplicationMain.class.getName() + " [nodes(default 3)]");
        if(args.length > 0) {
            nodes = Integer.parseInt(args[0]);
        }
        logger.info("Nodes: {}", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
        }
        List<KvServer> kvServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            Path workingDir = Paths.get(System.getProperty("user.dir")).resolve("journalkeeper").resolve("server" + i);
            FileUtils.deleteDirectory(workingDir.toFile());
            Properties properties = new Properties();
            properties.put("working_dir", workingDir.toString());
            KvServer kvServer = new KvServer( properties);
            kvServers.add(kvServer);
            kvServer.init(serverURIs.get(i), serverURIs);
            kvServer.recover();
            kvServer.start();
        }

        AdminClient adminClient = kvServers.get(0).getAdminClient();
        adminClient.waitClusterReady(0).get();

        List<KvClient> kvClients = kvServers.stream().map(KvServer::createClient).collect(Collectors.toList());



        for (int j = 0; j < 10; j++) {
            int i = j;
            logger.info("SET {} {}", "key1", "hello!");
            kvClients.get(i++ % serverURIs.size()).set("key1", "hello!");

            logger.info("SET {} {}", "key2", "hello!");
            kvClients.get(i++ % serverURIs.size()).set("key2", "world!");

            logger.info("GET {}", "key1");
            logger.info("Result: {}", kvClients.get(i++ % serverURIs.size()).get("key1"));

            logger.info("KEYS");
            logger.info("Result: {}", kvClients.get(i++ % serverURIs.size()).listKeys());

            logger.info("DEL key2");
            kvClients.get(i++ % serverURIs.size()).del("key2");

            logger.info("GET {}", "key2");
            logger.info("Result: {}", kvClients.get(i++ % serverURIs.size()).get("key2"));

            logger.info("KEYS");
            logger.info("Result: {}", kvClients.get(i ++ % serverURIs.size()).listKeys());
        }

        kvServers.parallelStream().forEach(KvServer::stop);

    }

}
