package com.jd.journalkeeper.examples.kv;

import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.JournalKeeperClient;
import com.jd.journalkeeper.core.api.JournalKeeperServer;
import com.jd.journalkeeper.core.exception.NoLeaderException;
import com.jd.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvServer implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(KvServer.class);
    private final BootStrap<KvEntry, KvQuery, KvResult> bootStrap;
    private final Properties properties;
    private final URI uri;
    private final List<URI> voters;

    public KvServer(URI uri, List<URI> voters, Properties properties) {
        this.properties = properties;
        this.uri = uri;
        this.voters = voters;
        bootStrap =
                new BootStrap<>(JournalKeeperServer.Roll.VOTER,
                        new KvStateFactory(),
                        new JsonSerializer<>(KvEntry.class),
                        new JsonSerializer<>(KvQuery.class),
                        new JsonSerializer<>(KvResult.class), properties);

    }



    public void recover() throws IOException {
        bootStrap.getServer().init(uri, voters);
        bootStrap.getServer().recover();
    }

    @Override
    public void start() {
        bootStrap.getServer().start();
    }

    public void waitForLeaderReady() {
        // 等待选出leader
        URI leader = null;
        while (leader == null) {
            try {
                leader = bootStrap.getClient().getServers().get().getLeader();
                Thread.sleep(10);
            } catch (NoLeaderException ignored) {}
            catch (Exception e) {
                logger.warn("Exception:", e);
            }
        }
    }

    @Override
    public void stop() {
        bootStrap.shutdown();

    }

    @Override
    public ServerState serverState() {
        return bootStrap.getServer().serverState();
    }

    public KvClient getClient() {
        return new KvClient(bootStrap.getClient());
    }
}
