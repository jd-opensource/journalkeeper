package com.jd.journalkeeper.coordinating.server.config;

import com.jd.journalkeeper.coordinating.server.util.PropertiesUtils;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * CoordinatingConfigParser
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingConfigParser {

    public CoordinatingConfig parse(Properties properties) {
        CoordinatingConfig config = new CoordinatingConfig();
        config.setServer(parseServerConfig(properties));
        config.setKeeper(parseKeeperConfig(properties));
        config.setWatcher(parseWatcherConfig(properties));
        config.setProperties(properties);
        return config;
    }

    protected ServerConfig parseServerConfig(Properties properties) {
        ServerConfig config = new ServerConfig();
        config.setHost(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_HOST, config.getHost()));
        config.setPort(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_PORT, CoordinatingConfigs.DEFAULT_SERVER_PORT));
        config.setAcceptThread(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_ACCEPT_THREAD, config.getAcceptThread()));
        config.setIoThread(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_IO_THREAD, config.getIoThread()));
        config.setMaxIdleTime(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_MAX_IDLE_TIME, config.getMaxIdleTime()));
        config.setReuseAddress(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_REUSE_ADDRESS, config.isReuseAddress()));
        config.setSoLinger(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_SO_LINGER, config.getSoLinger()));
        config.setTcpNoDelay(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_TCP_NO_DELAY, config.isTcpNoDelay()));
        config.setKeepAlive(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_KEEPALIVE, config.isKeepAlive()));
        config.setSoTimeout(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_SO_TIMEOUT, config.getSoTimeout()));
        config.setSocketBufferSize(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_SOCKET_BUFFER_SIZE, config.getSocketBufferSize()));
        config.setFrameMaxSize(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_FRAME_MAX_SIZE, config.getFrameMaxSize()));
        config.setBacklog(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_BACKLOG, config.getBacklog()));
        config.setMaxOneway(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_MAX_ONEWAY, config.getMaxOneway()));
        config.setNonBlockOneway(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_NONBLOCK_ONEWAY, config.isNonBlockOneway()));
        config.setMaxAsync(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_MAX_ASYNC, config.getMaxAsync()));
        config.setCallbackThreads(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_CALLBACK_THREADS, config.getCallbackThreads()));
        config.setSendTimeout(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_SEND_TIMEOUT, config.getSendTimeout()));
        config.setMaxRetrys(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_MAX_RETRYS, config.getMaxRetrys()));
        config.setMaxRetryDelay(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_MAX_RETRY_DELAY, config.getMaxRetryDelay()));
        config.setRetryDelay(PropertiesUtils.getProperty(properties, CoordinatingConfigs.SERVER_RETRY_DELAY, config.getRetryDelay()));
        return config;
    }

    protected CoordinatingKeeperConfig parseKeeperConfig(Properties properties) {
        CoordinatingKeeperConfig config = new CoordinatingKeeperConfig();
        String host = properties.getProperty(CoordinatingKeeperConfigs.HOST, CoordinatingKeeperConfigs.DEFAULT_HOST);
        int port = PropertiesUtils.getProperty(properties, CoordinatingKeeperConfigs.PORT, CoordinatingKeeperConfigs.DEFAULT_PORT);
        String cluster = properties.getProperty(CoordinatingKeeperConfigs.CLUSTER);
        config.setCurrent(URI.create(String.format("coordinating://%s:%s", host, port)));

        if (StringUtils.isBlank(cluster)) {
            config.setCluster(Arrays.asList(config.getCurrent()));
        } else {
            config.setCluster(new ArrayList<>());
            String[] nodes = cluster.split(CoordinatingKeeperConfigs.CLUSTER_SEPARATOR);
            for (String node : nodes) {
                String[] hostAndPort = node.split(CoordinatingKeeperConfigs.CLUSTER_PORT_SEPARATOR);
                if (hostAndPort.length != 2) {
                    continue;
                }
                config.getCluster().add(URI.create(String.format("coordinating://%s:%s", hostAndPort[0], hostAndPort[1])));
            }
        }

        String role = PropertiesUtils.getProperty(properties, CoordinatingKeeperConfigs.ROLE, RaftServer.Roll.VOTER);
        config.setRole(role.equals(String.valueOf(RaftServer.Roll.VOTER)) ? RaftServer.Roll.VOTER : RaftServer.Roll.OBSERVER);
        return config;
    }

    protected CoordinatingWatcherConfig parseWatcherConfig(Properties properties) {
        CoordinatingWatcherConfig config = new CoordinatingWatcherConfig();
        config.setPublishTimeout(PropertiesUtils.getProperty(properties, CoordinatingConfigs.WATCHER_PUBLISH_TIMEOUT, config.getPublishTimeout()));
        return config;
    }
}