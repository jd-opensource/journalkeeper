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
package io.journalkeeper.examples.kv;

import io.journalkeeper.core.serialize.WrappedBootStrap;
import io.journalkeeper.core.serialize.WrappedRaftClient;
import io.journalkeeper.utils.net.NetworkingUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KvExampleApplicationMain {
    private static final Logger logger = LoggerFactory.getLogger(KvExampleApplicationMain.class);

    public static void main(String[] args) throws Exception {
        int nodes = 1;
        logger.info("Usage: java " + KvExampleApplicationMain.class.getName() + " [nodes(default 3)]");
        if (args.length > 0) {
            nodes = Integer.parseInt(args[0]);
        }
        logger.info("Nodes: {}", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("jk://localhost:" + NetworkingUtils.findRandomOpenPortOnAllLocalInterfaces());
            serverURIs.add(uri);
        }

        List<WrappedBootStrap<String, String, String, String>> serverBootStraps = new ArrayList<>(serverURIs.size());

        KvStateFactory stateFactory = new KvStateFactory();
        for (int i = 0; i < serverURIs.size(); i++) {
            Path workingDir = Paths.get(System.getProperty("user.dir")).resolve("journalkeeper").resolve("server" + i);
            FileUtils.deleteDirectory(workingDir.toFile());
            Properties properties = new Properties();
            properties.put("working_dir", workingDir.toString());
            WrappedBootStrap<String, String, String, String> bootStrap = new WrappedBootStrap<>(stateFactory, properties);
            bootStrap.getServer().init(serverURIs.get(i), serverURIs);
            bootStrap.getServer().recover();
            bootStrap.getServer().start();
            serverBootStraps.add(bootStrap);
        }

        WrappedBootStrap<String, String, String, String> clientBootStrap = new WrappedBootStrap<>(serverURIs, new Properties());

        WrappedRaftClient<String, String, String, String> kvClient = clientBootStrap.getClient();
        kvClient.waitForClusterReady();
        logger.info("SET key1 hello!");
        String result = kvClient.update("SET key1 hello!").get();
        logger.info(result);

        logger.info("SET key2 world!");
        result = kvClient.update("SET key2 world!").get();
        logger.info(result);

        logger.info("GET key1");
        result = kvClient.query("GET key1").get();
        logger.info(result);

        logger.info("KEYS");
        result = kvClient.query("KEYS").get();
        logger.info(result);

        logger.info("DEL key2");
        result = kvClient.update("DEL key2").get();
        logger.info(result);

        logger.info("GET key2");
        result = kvClient.query("GET key2").get();
        logger.info(result);

        logger.info("KEYS");
        result = kvClient.query("KEYS").get();
        logger.info(result);

        clientBootStrap.shutdown();
        serverBootStraps.forEach(WrappedBootStrap::shutdown);
    }

}
