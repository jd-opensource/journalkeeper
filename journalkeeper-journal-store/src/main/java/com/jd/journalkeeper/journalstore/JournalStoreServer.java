package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.BootStrap;
import com.jd.journalkeeper.core.api.RaftServer;
import com.jd.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreServer implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreServer.class);
    private final BootStrap<byte [], JournalStoreQuery, JournalStoreQueryResult> bootStrap;

    public JournalStoreServer(Properties properties) {
        bootStrap = new BootStrap<>(
                RaftServer.Roll.VOTER,
                new JournalStoreStateFactory(),
                new ByteArraySerializer(),
                new JournalStoreQuerySerializer(),
                new JournalStoreQueryResultSerializer(),
                properties
        );
    }

    public void init(URI uri, List<URI> voters) throws IOException {
        bootStrap.getServer().init(uri, voters);
    }

    public void recover() throws IOException {
        bootStrap.getServer().recover();
    }


    @Override
    public void start() {
        bootStrap.getServer().start();
    }

    @Override
    public void stop() {
        bootStrap.shutdown();
    }

    @Override
    public ServerState serverState() {
        return bootStrap.getServer().serverState();
    }

    public JournalStoreClient createClient() {
        return new JournalStoreClient(bootStrap.getClient());
    }
}
