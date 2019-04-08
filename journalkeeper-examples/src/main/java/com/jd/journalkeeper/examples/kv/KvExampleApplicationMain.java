package com.jd.journalkeeper.examples.kv;

import com.jd.journalkeeper.utils.net.NetworkingUtils;
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
import java.util.stream.Collectors;

public class KvExampleApplicationMain {
    private static final Logger logger = LoggerFactory.getLogger(KvExampleApplicationMain.class);
    public static void main(String [] args) throws IOException {
        int nodes = 3;
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
        kvServers.get(0).waitForLeaderReady();
        List<KvClient> kvClients = kvServers.stream().map(KvServer::createClient).collect(Collectors.toList());


        int i = 0;
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

        kvServers.parallelStream().forEach(KvServer::stop);

    }

}
