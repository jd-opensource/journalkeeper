package com.jd.journalkeeper.coordinating.server;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfiguration;
import com.jd.journalkeeper.coordinating.server.network.CoordinatingServer;
import com.jd.journalkeeper.rpc.remoting.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoordinatingService
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingService extends Service {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingKeeperServer.class);

    private String[] args;
    private CoordinatingConfiguration configuration;
    private CoordinatingKeeperServer keeperServer;
    private CoordinatingServer server;
    private CoordinatingContext context;

    public CoordinatingService(String[] args) {
        this.args = args;
    }

    @Override
    protected void validate() throws Exception {
        configuration = new CoordinatingConfiguration(args);
        CoordinatingConfig config = configuration.getConfig();
        logger.info("service configs: {}", config);

        keeperServer = new CoordinatingKeeperServer(config.getKeeper().getCurrent(), config.getKeeper().getCluster(), config.getKeeper().getRole(), config.getProperties());
        context = new CoordinatingContext(config, keeperServer);
        server = new CoordinatingServer(context);
    }

    @Override
    protected void doStart() throws Exception {
        keeperServer.start();
        server.start();

        logger.info("service is started");
    }

    @Override
    protected void doStop() {
        server.stop();
        keeperServer.stop();

        logger.info("service is stopped");
    }
}