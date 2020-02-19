/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.sql.server;

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.serializer.KryoSerializer;
import io.journalkeeper.sql.state.SQLStateFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * SQLServerAccessPoint
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class SQLServerAccessPoint {

    private Properties config;
    private SQLStateFactory stateFactory;

    public SQLServerAccessPoint(Properties config) {
        this(config,
                new SQLStateFactory(
                        new KryoSerializer<>(WriteRequest.class),
                        new KryoSerializer<>(WriteResponse.class),
                        new KryoSerializer<>(ReadRequest.class),
                        new KryoSerializer<>(ReadResponse.class)));
    }

    public SQLServerAccessPoint(Properties config,
                                SQLStateFactory stateFactory) {
        this.config = config;
        this.stateFactory = stateFactory;
    }

    public SQLServer createServer(URI current, List<URI> servers, RaftServer.Roll role) {
        return new SQLServer(current, servers, config, role, stateFactory);
    }

    public SQLServer createRemoteServer(URI current, List<URI> servers) {
        return new SQLServer(servers, config, stateFactory);
    }
}