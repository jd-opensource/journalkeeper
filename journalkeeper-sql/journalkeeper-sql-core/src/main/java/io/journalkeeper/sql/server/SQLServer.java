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
package io.journalkeeper.sql.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.sql.client.SQLClient;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * SQLServer
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class SQLServer implements StateServer {

    protected static final Logger logger = LoggerFactory.getLogger(SQLServer.class);

    private URI current;
    private List<URI> servers;
    private RaftServer.Roll role;
    private Properties config;

    private BootStrap<WriteRequest, WriteResponse, ReadRequest, ReadResponse> bootStrap;
    private volatile SQLClient client;

    public SQLServer(URI current, List<URI> servers, Properties config,
                     RaftServer.Roll role,
                     StateFactory<WriteRequest, WriteResponse, ReadRequest, ReadResponse> stateFactory,
                     Serializer<WriteRequest> writeRequestSerializer,
                     Serializer<WriteResponse> writeResponseSerializer,
                     Serializer<ReadRequest> readRequestSerializer,
                     Serializer<ReadResponse> readResponseSerializer) {
        this.current = current;
        this.servers = servers;
        this.role = role;
        this.config = config;
        this.bootStrap = new BootStrap<>(role, stateFactory, writeRequestSerializer, writeResponseSerializer,
                readRequestSerializer, readResponseSerializer, config);
    }

    public URI getCurrent() {
        return current;
    }

    public List<URI> getServers() {
        return servers;
    }

    public RaftServer.Roll getRole() {
        return role;
    }


    @Override
    public void start() {
        try {
            bootStrap.getServer().init(current, servers);
            bootStrap.getServer().recover();
            bootStrap.getServer().start();
        } catch (Exception e) {
            throw new SQLException(e);
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
}