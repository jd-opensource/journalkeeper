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
package io.journalkeeper.core;

import io.journalkeeper.core.api.AdminClient;
import io.journalkeeper.core.api.ClusterAccessPoint;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.client.ClientRpc;
import io.journalkeeper.core.client.DefaultAdminClient;
import io.journalkeeper.core.client.DefaultRaftClient;
import io.journalkeeper.core.client.LocalClientRpc;
import io.journalkeeper.core.client.RemoteClientRpc;
import io.journalkeeper.core.entry.DefaultJournalEntryParser;
import io.journalkeeper.core.server.Server;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.RpcException;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.utils.retry.IncreasingRetryPolicy;
import io.journalkeeper.utils.retry.RetryPolicy;
import io.journalkeeper.utils.spi.ServiceSupport;
import io.journalkeeper.utils.state.StateServer;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author LiYue
 * Date: 2019-03-25
 */
public class BootStrap implements ClusterAccessPoint {
    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);
    private final static int SCHEDULE_EXECUTOR_QUEUE_SIZE = 128;

    private final StateFactory stateFactory;
    private final Properties properties;
    private ScheduledExecutorService serverScheduledExecutor, clientScheduledExecutor;
    private ExecutorService serverAsyncExecutor, clientAsyncExecutor;
    private final RaftServer.Roll roll;
    private final RpcAccessPointFactory rpcAccessPointFactory;
    private final List<URI> servers;
    private final Server server;
    private RaftClient client = null;
    private AdminClient adminClient = null;
    private RaftClient localClient = null;
    private AdminClient localAdminClient = null;
    private final JournalEntryParser journalEntryParser;
    private final RetryPolicy remoteRetryPolicy =
            new IncreasingRetryPolicy(
                    new long [] {50, 50, 50, 50, 50,
                            100, 100, 100, 100, 100, 100,
                            300, 300, 300, 300, 300, 300,
                            500, 500, 500, 500, 500, 500,
                            1000, 1000, 1000, 1000, 1000,
                            5000, 5000, 5000, 5000, 5000}, 50);
    private final boolean isExecutorProvided;

    /**
     * 初始化远程模式的BootStrap，本地没有任何Server，所有操作直接请求远程Server。
     * @param servers 远程Server 列表
     * @param properties 配置属性
     */
    public BootStrap(List<URI> servers,
                     Properties properties) {
        this(null, servers, null, null,
                null, null, null, null,
                properties);
    }

    /**
     * 初始化远程模式的BootStrap，本地没有任何Server，所有操作直接请求远程Server。
     * @param servers 远程Server 列表
     * @param clientAsyncExecutor 用于执行异步任务的Executor
     * @param clientScheduledExecutor 用于执行定时任务的Executor
     * @param properties 配置属性
     */
    public BootStrap(List<URI> servers,
                     ExecutorService clientAsyncExecutor, ScheduledExecutorService clientScheduledExecutor,
                     Properties properties) {
        this(null, servers, null, null,
                clientAsyncExecutor, clientScheduledExecutor, null, null,
                properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，请求本地Server通信。
     * @param roll 本地Server的角色。
     * @param stateFactory 状态机工厂，用户创建状态机实例
     * @param properties 配置属性
     */
    public BootStrap(RaftServer.Roll roll, StateFactory stateFactory,
                     Properties properties) {
        this(roll, null, stateFactory, new DefaultJournalEntryParser(), null, null,null, null,
                properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，请求本地Server通信。
     * @param roll 本地Server的角色。
     * @param stateFactory 状态机工厂，用户创建状态机实例
     * @param journalEntryParser 操作日志的解析器，一般不需要提供，使用默认解析器即可。
     * @param properties 配置属性
     */
    public BootStrap(RaftServer.Roll roll, StateFactory stateFactory,
                     JournalEntryParser journalEntryParser,
                     Properties properties) {
        this(roll, null, stateFactory, journalEntryParser,
                null, null, null, null,
                properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，请求本地Server通信。
     * @param roll 本地Server的角色。
     * @param stateFactory 状态机工厂，用户创建状态机实例
     * @param journalEntryParser 操作日志的解析器，一般不需要提供，使用默认解析器即可。
     * @param clientAsyncExecutor Client用于执行异步任务的Executor
     * @param clientScheduledExecutor Client用于执行定时任务的Executor
     * @param serverAsyncExecutor Server用于执行异步任务的Executor
     * @param serverScheduledExecutor Server用于执行定时任务的Executor
     * @param properties 配置属性
     */
    public BootStrap(RaftServer.Roll roll, StateFactory stateFactory,
                     JournalEntryParser journalEntryParser,
                     ExecutorService clientAsyncExecutor,
                     ScheduledExecutorService clientScheduledExecutor,
                     ExecutorService serverAsyncExecutor,
                     ScheduledExecutorService serverScheduledExecutor,
                     Properties properties) {
        this(roll, null, stateFactory,
                journalEntryParser,
                clientAsyncExecutor, clientScheduledExecutor, serverAsyncExecutor, serverScheduledExecutor,
                properties);
    }


    private BootStrap(RaftServer.Roll roll, List<URI> servers, StateFactory stateFactory,
                      JournalEntryParser journalEntryParser,
                      ExecutorService clientAsyncExecutor,
                      ScheduledExecutorService clientScheduledExecutor,
                      ExecutorService serverAsyncExecutor,
                      ScheduledExecutorService serverScheduledExecutor,
                      Properties properties) {
        this.stateFactory = stateFactory;
        this.properties = properties;
        this.roll = roll;
        this.rpcAccessPointFactory = ServiceSupport.load(RpcAccessPointFactory.class);
        this.isExecutorProvided = (
                clientAsyncExecutor != null ||
                        clientScheduledExecutor != null ||
                        serverAsyncExecutor != null ||
                        serverScheduledExecutor != null);
        this.journalEntryParser = journalEntryParser;
        this.clientAsyncExecutor = clientAsyncExecutor;
        this.serverAsyncExecutor = serverAsyncExecutor;
        this.clientScheduledExecutor = clientScheduledExecutor;
        this.serverScheduledExecutor = serverScheduledExecutor;
        this.server = createServer();
        this.servers = servers;
    }

    private Server createServer() {
        if(null == serverScheduledExecutor && !isExecutorProvided) {
            this.serverScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Server-Scheduled-Executor"));
        }
        if(null == serverAsyncExecutor && !isExecutorProvided) {
            this.serverAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Server-Async-Executor"));
        }

        if(null != roll) {
            return new Server(roll, stateFactory, journalEntryParser, serverScheduledExecutor, serverAsyncExecutor, properties);
        }
        return null;
    }

    @Override
    public RaftClient getClient() {
        if(null == client) {
            RemoteClientRpc clientRpc = createRemoteClientRpc();
            client = new DefaultRaftClient(clientRpc, properties);
        }
        return client;
    }

    @Override
    public RaftClient getLocalClient() {

        if(null == localClient) {
            LocalClientRpc clientRpc = createLocalClientRpc();
            localClient = new DefaultRaftClient(clientRpc, properties);
        }
        return localClient;
    }

    private LocalClientRpc createLocalClientRpc() {
        if(null == clientAsyncExecutor && !isExecutorProvided) {
            this.clientAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Client-Async-Executor"));
        }
        if(null == clientScheduledExecutor && !isExecutorProvided) {
            this.clientScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Client-Scheduled-Executor"));
        }

        if(this.server != null) {
            return new LocalClientRpc(server, remoteRetryPolicy, clientAsyncExecutor, clientScheduledExecutor);
        } else {
            throw new IllegalStateException("No local server!");
        }
    }

    private RemoteClientRpc createRemoteClientRpc() {
        if(null == clientAsyncExecutor && !isExecutorProvided) {
            this.clientAsyncExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("JournalKeeper-Client-Async-Executor"));
        }
        if(null == clientScheduledExecutor && !isExecutorProvided) {
            this.clientScheduledExecutor = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_QUEUE_SIZE, new NamedThreadFactory("JournalKeeper-Client-Scheduled-Executor"));
        }

        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPointFactory.createClientServerRpcAccessPoint(this.properties);
        RemoteClientRpc clientRpc;
        if(this.server == null) {
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy, clientAsyncExecutor, clientScheduledExecutor);
        } else {
            clientServerRpcAccessPoint = new LocalDefaultRpcAccessPoint(server, clientServerRpcAccessPoint);
            clientRpc = new RemoteClientRpc(getServersForClient(), clientServerRpcAccessPoint, remoteRetryPolicy, clientAsyncExecutor, clientScheduledExecutor);
            clientRpc.setPreferredServer(server.serverUri());
        }
        return clientRpc;
    }

    public void shutdown() {

        if(null != client) {
            client.stop();
        }
        if(null != adminClient) {
            adminClient.stop();
        }
        if(null != server ) {
            StateServer.ServerState state;
            if( (state = server.serverState()) == StateServer.ServerState.RUNNING) {
                server.stop();
            } else {
                logger.warn("Server {} state is {}, will not stop!", server.serverUri(), state);
            }
        }
        if(!isExecutorProvided) {
            shutdownExecutorService(serverScheduledExecutor);
            shutdownExecutorService(serverAsyncExecutor);
            shutdownExecutorService(clientScheduledExecutor);
            shutdownExecutorService(clientAsyncExecutor);
        }
    }

    @Override
    public RaftServer getServer() {
        return server;
    }

    @Override
    public AdminClient getAdminClient() {
        if(null == adminClient) {
            ClientRpc clientRpc = createRemoteClientRpc();
            adminClient = new DefaultAdminClient(clientRpc, properties);
        }
        return adminClient;
    }

    @Override
    public AdminClient getLocalAdminClient() {
        if(null == localAdminClient) {
            ClientRpc clientRpc = createLocalClientRpc();
            localAdminClient = new DefaultAdminClient(clientRpc, properties);
        }
        return localAdminClient;
    }


    private List<URI> getServersForClient() {
        if(null == server) {
            return servers;
        } else {
            try {
                return server.getServers().get().getClusterConfiguration().getVoters();
            } catch (Throwable e) {
                throw new RpcException(e);
            }
        }
    }

    private void shutdownExecutorService(ExecutorService executor) {
        if(null != executor) {
            executor.shutdown();
        }
    }
}
