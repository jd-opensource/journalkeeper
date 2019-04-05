package com.jd.journalkeeper.examples.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class SingleNodeServerMain {
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeServerMain.class);
    public static void main(String [] args) throws IOException {
        URI serverUri = URI.create("jk://localhost:32551");
        Properties properties = new Properties();

        KvServer kvServer = new KvServer(serverUri, Collections.singletonList(serverUri), properties);
        kvServer.recover();
        kvServer.start();

        kvServer.waitForLeaderReady();
        KvClient client = kvServer.getClient();

        logger.info("SET {} {}", "key1", "hello!");
        client.set("key1", "hello!");

        logger.info("SET {} {}", "key2", "hello!");
        client.set("key2", "world!");

        logger.info("GET {}", "key1");
        logger.info("Result: {}", client.get("key1"));

        logger.info("KEYS");
        logger.info("Result: {}", client.listKeys());

        logger.info("DEL key2");
        client.del("key2");

        logger.info("GET {}", "key2");
        logger.info("Result: {}", client.get("key2"));

        logger.info("KEYS");
        logger.info("Result: {}", client.listKeys());
        kvServer.stop();

    }
}
