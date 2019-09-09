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

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.exception.NoLeaderException;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-04-03
 */
public class KvServer implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(KvServer.class);
    private final BootStrap<KvEntry, Void, KvQuery, KvResult> bootStrap;

    public KvServer(Properties properties) {
        this(RaftServer.Roll.VOTER, properties);

    }

    public KvServer(RaftServer.Roll roll, Properties properties) {
        bootStrap =
                new BootStrap<>(roll,
                        new KvStateFactory(),
                        new JsonSerializer<>(KvEntry.class),
                        new JsonSerializer<>(Void.class),
                        new JsonSerializer<>(KvQuery.class),
                        new JsonSerializer<>(KvResult.class), properties);

    }

    public void init(URI uri, List<URI> voters) throws IOException {
        bootStrap.getServer().init(uri, voters);
    }

    public void recover() throws IOException {
        bootStrap.getServer().recover();
    }

    public List<URI> getParents() {
        return bootStrap.getServer().getParents();
    }

    public AdminClient getAdminClient() {
        return bootStrap.getAdminClient();
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

    public KvClient createClient() {
        return new KvClient(bootStrap.getClient());
    }
    public AdminClient createAdminClient() {
        return bootStrap.getAdminClient();
    }

    public RaftServer.Roll roll() {
        return this.bootStrap.getServer().roll();
    }

    public URI serverUri() {
        return this.bootStrap.getServer().serverUri();
    }
}
