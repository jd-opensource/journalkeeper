package io.journalkeeper.core;

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.QueryConsistency;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.serialize.WrappedBootStrap;
import io.journalkeeper.core.serialize.WrappedRaftClient;
import io.journalkeeper.core.serialize.WrappedState;
import io.journalkeeper.core.serialize.WrappedStateFactory;
import io.journalkeeper.utils.test.TestPathUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author LiYue
 * Date: 2020/2/19
 */
public class ConsistencyTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsistencyTest.class);
    private void stopServers(List<WrappedBootStrap<Integer, Integer, Integer, Integer>> kvServers) {
        for (WrappedBootStrap<Integer, Integer, Integer, Integer> serverBootStraps : kvServers) {
            try {
                serverBootStraps.shutdown();
            } catch (Throwable t) {
                logger.warn("Stop server {} exception:", serverBootStraps.getServer().serverUri(), t);
            }
        }
    }

    @Test
    public void testSequential() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("TestSequential");
        List<WrappedBootStrap<Integer, Integer, Integer, Integer>> serverBootStraps = createServers(3, path);
        try {
            for (int j = 0; j < 3; j++) {
                WrappedRaftClient<Integer, Integer, Integer, Integer> client = serverBootStraps.get(j).getClient();
                for (int i = 0; i < 100; i++) {
                    Integer value = client.update(1).get();
                    Assert.assertEquals(value, client.query(null, QueryConsistency.SEQUENTIAL).get());
                }
            }
        } finally {
            stopServers(serverBootStraps);
        }
    }

    @Test
    public void testAvailability() throws Exception {
        Path path = TestPathUtils.prepareBaseDir("TestSequential");
        List<WrappedBootStrap<Integer, Integer, Integer, Integer>> serverBootStraps = createServers(3, path);
        List<URI> serverUris = serverBootStraps.stream().map(b -> b.getServer().serverUri()).collect(Collectors.toList());
        WrappedBootStrap<Integer, Integer, Integer, Integer> clientBootStrap = new WrappedBootStrap<Integer, Integer, Integer, Integer>(serverUris, new Properties());
        WrappedRaftClient<Integer, Integer, Integer, Integer> client = clientBootStrap.getClient();
        try {
            for (int i = 0; i < 100; i++) {
                client.update(1).get();
            }
            List<WrappedBootStrap<Integer, Integer, Integer, Integer>> tobeStopped = serverBootStraps.subList(0, 2);
            stopServers(tobeStopped);
            serverBootStraps.removeAll(tobeStopped);
            Assert.assertTrue(client.query(null, QueryConsistency.NONE).get() >= 0);

        } finally {
            clientBootStrap.shutdown();
            stopServers(serverBootStraps);
        }
    }

    private List<WrappedBootStrap<Integer, Integer, Integer, Integer>> createServers(int nodes, Path path) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        return createServers(nodes, path, RaftServer.Roll.VOTER, true);
    }

    private List<WrappedBootStrap<Integer, Integer, Integer, Integer>> createServers(int nodes, Path path, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        logger.info("Create {} nodes servers", nodes);
        List<URI> serverURIs = new ArrayList<>(nodes);
        List<Properties> propertiesList = new ArrayList<>(nodes);
        for (int i = 0; i < nodes; i++) {
            URI uri = URI.create("local://test" + i);
            serverURIs.add(uri);
            Path workingDir = path.resolve("server" + i);
            Properties properties = new Properties();
            properties.setProperty("working_dir", workingDir.toString());
            properties.setProperty("persistence.journal.file_data_size", String.valueOf(128 * 1024));
            properties.setProperty("persistence.index.file_data_size", String.valueOf(16 * 1024));
            properties.setProperty("disable_logo", "true");

//            properties.setProperty("enable_metric", "true");
//            properties.setProperty("print_metric_interval_sec", "3");
            properties.setProperty("print_state_interval_sec", String.valueOf(5));

            propertiesList.add(properties);
        }
        return createServers(serverURIs, propertiesList, roll, waitForLeader);

    }


    private List<WrappedBootStrap<Integer, Integer, Integer, Integer>> createServers(List<URI> serverURIs, List<Properties> propertiesList, RaftServer.Roll roll, boolean waitForLeader) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        List<WrappedBootStrap<Integer, Integer, Integer, Integer>> serverBootStraps = new ArrayList<>(serverURIs.size());
        for (int i = 0; i < serverURIs.size(); i++) {
            WrappedBootStrap<Integer, Integer, Integer, Integer> serverBootStrap = new WrappedBootStrap<>(roll, new ConsistencyStateFactory(), propertiesList.get(i));
            serverBootStraps.add(serverBootStrap);

            serverBootStrap.getServer().init(serverURIs.get(i), serverURIs);
            serverBootStrap.getServer().recover();
            serverBootStrap.getServer().start();
        }
        if (waitForLeader) {
            serverBootStraps.get(0).getAdminClient().waitForClusterReady();
        }
        return serverBootStraps;
    }

    private static class ConsistencyState implements WrappedState<Integer, Integer, Integer, Integer> {
        private int value = 0;
        @Override
        public Integer execute(Integer entry) {
            return value += entry;
        }

        @Override
        public Integer query(Integer query) {
            return value;
        }

        @Override
        public void recover(Path path, Properties properties) throws IOException {

        }
    }

    private static class ConsistencyStateFactory implements WrappedStateFactory<Integer, Integer, Integer, Integer> {

        @Override
        public WrappedState<Integer, Integer, Integer, Integer> createState() {
            return new ConsistencyState();
        }
    }
}
