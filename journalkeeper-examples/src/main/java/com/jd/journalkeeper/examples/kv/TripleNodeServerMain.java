package com.jd.journalkeeper.examples.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TripleNodeServerMain {
    private static final Logger logger = LoggerFactory.getLogger(TripleNodeServerMain.class);
    public static void main(String [] args) throws IOException {
        List<URI> serverURIs = Arrays.asList(
                URI.create("jk://localhost:32551"),
                URI.create("jk://localhost:32552"),
                URI.create("jk://localhost:32553"));
        List<KvServer> kvServers = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {

            Properties properties = new Properties();
            properties.put("working_dir", Paths.get(System.getProperty("user.dir")).resolve("journalkeeper").resolve("server" + i).toString());
//            properties.put("heartbeat_interval_ms", "5000");
//            properties.put("election_timeout_ms", "50000");
            KvServer kvServer = new KvServer(serverURIs.get(i), serverURIs, properties);
            kvServers.add(kvServer);
            kvServer.recover();
            kvServer.start();
        }
        kvServers.get(0).waitForLeaderReady();
        List<KvClient> kvClients = kvServers.stream().map(KvServer::getClient).collect(Collectors.toList());



        logger.info("SET {} {}", "key1", "hello!");
        kvClients.get(0).set("key1", "hello!");

        logger.info("SET {} {}", "key2", "hello!");
        kvClients.get(1).set("key2", "world!");

        logger.info("GET {}", "key1");
        logger.info("Result: {}", kvClients.get(2).get("key1"));

        logger.info("KEYS");
        logger.info("Result: {}", kvClients.get(0).listKeys());

        logger.info("DEL key2");
        kvClients.get(1).del("key2");

        logger.info("GET {}", "key2");
        logger.info("Result: {}", kvClients.get(2).get("key2"));

        logger.info("KEYS");
        logger.info("Result: {}", kvClients.get(0).listKeys());
        for(KvServer kvServer: kvServers) {
            kvServer.stop();
        }

    }

}
