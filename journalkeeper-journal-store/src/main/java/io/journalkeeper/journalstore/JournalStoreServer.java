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
package io.journalkeeper.journalstore;

import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class JournalStoreServer implements StateServer {
    private static final Logger logger = LoggerFactory.getLogger(JournalStoreServer.class);
    private final BootStrap<byte [], Long, JournalStoreQuery, JournalStoreQueryResult> bootStrap;

    public JournalStoreServer(Properties properties) {
        bootStrap = new BootStrap<>(
                RaftServer.Roll.VOTER,
                new JournalStoreStateFactory(),
                new ByteArraySerializer(),
                new LongSerializer(),
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

    public URI serverUri() {
        return bootStrap.getServer().serverUri();
    }
}
