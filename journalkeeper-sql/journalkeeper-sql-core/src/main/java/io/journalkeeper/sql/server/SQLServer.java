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

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.BootStrap;
import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.sql.client.SQLClient;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.exception.SQLException;
import io.journalkeeper.sql.state.SQLStateFactory;
import io.journalkeeper.utils.state.StateServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * SQLServer
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class SQLServer implements StateServer {

    protected static final Logger logger = LoggerFactory.getLogger(SQLServer.class);
    private final Serializer<WriteRequest> writeRequestSerializer;
    private final Serializer<WriteResponse> writeResponseSerializer;
    private final Serializer<ReadRequest> readRequestSerializer;
    private final Serializer<ReadResponse> readResponseSerializer;
    private URI current;
    private List<URI> servers;
    private RaftServer.Roll role;
    private Properties config;
    private BootStrap bootStrap;
    private volatile SQLClient client;

    public SQLServer(URI current, List<URI> servers, Properties config,
                     RaftServer.Roll role,
                     SQLStateFactory stateFactory) {
        this.writeRequestSerializer = stateFactory.getWriteRequestSerializer();
        this.writeResponseSerializer = stateFactory.getWriteResponseSerializer();
        this.readRequestSerializer = stateFactory.getReadRequestSerializer();
        this.readResponseSerializer = stateFactory.getReadResponseSerializer();

        this.current = current;
        this.servers = servers;
        this.role = role;
        this.config = config;
        this.bootStrap = new BootStrap(role, stateFactory, config);
    }

    public SQLServer(List<URI> servers, Properties config, SQLStateFactory stateFactory) {
        this.servers = servers;
        this.config = config;
        this.writeRequestSerializer = stateFactory.getWriteRequestSerializer();
        this.writeResponseSerializer = stateFactory.getWriteResponseSerializer();
        this.readRequestSerializer = stateFactory.getReadRequestSerializer();
        this.readResponseSerializer = stateFactory.getReadResponseSerializer();
        this.bootStrap = new BootStrap(servers, config);
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

    public boolean waitClusterReady(long timeout, TimeUnit unit) {
        timeout = unit.toMillis(timeout);
        long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < timeout || timeout <= 0) {
            try {
                bootStrap.getClient().waitForClusterReady(1000);
                return true;
            } catch (TimeoutException e) {
            }
            logger.info("wait for cluster ready, current: {}, servers: {}", current, servers);
        }
        throw new SQLException(new TimeoutException());
    }

    public URI getLeader() {
        try {
            return bootStrap.getAdminClient().getClusterConfiguration().get().getLeader();
        } catch (Exception e) {
            return null;
        }
    }

    public SQLClient getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = new SQLClient(servers, config, bootStrap,
                            writeRequestSerializer, writeResponseSerializer, readRequestSerializer, readResponseSerializer);
                }
            }
        }
        return client;
    }

    @Override
    public void start() {
        try {
            bootStrap.getServer().start();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void tryStart() {
        try {
            RaftServer server = bootStrap.getServer();
            if (!server.isInitialized()) {
                server.init(current, servers);
            }
            server.recover();
            server.start();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void recover() {
        try {
            bootStrap.getServer().recover();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public boolean isInitialized() {
        return bootStrap.getServer().isInitialized();
    }

    public void init() {
        try {
            bootStrap.getServer().init(current, servers);
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

    public AdminClient getAdminClient() {
        return bootStrap.getAdminClient();
    }
}