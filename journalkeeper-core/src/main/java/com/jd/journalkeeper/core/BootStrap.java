package com.jd.journalkeeper.core;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.core.api.ClusterAccessPoint;
import com.jd.journalkeeper.core.api.JournalKeeperClient;
import com.jd.journalkeeper.core.api.JournalKeeperServer;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.client.Client;
import com.jd.journalkeeper.core.server.Observer;
import com.jd.journalkeeper.core.server.Server;
import com.jd.journalkeeper.core.server.Voter;
import com.jd.journalkeeper.rpc.RpcAccessPointFactory;
import com.jd.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import com.jd.journalkeeper.utils.spi.ServiceSupport;
import com.jd.journalkeeper.utils.threads.NamedThreadFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author liyue25
 * Date: 2019-03-25
 */
public class BootStrap<E, Q, R> implements ClusterAccessPoint<E, Q, R> {
    private final static int SCHEDULE_EXECUTOR_THREADS = 4;

    private final StateFactory<E, Q, R> stateFactory;
    private final Serializer<E> entrySerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<R> resultSerializer;
    private final Properties properties;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService asyncExecutorService;
    private final JournalKeeperServer.Roll roll;
    private final ClientServerRpcAccessPoint clientServerRpcAccessPoint;
    private final Server<E, Q, R> server;

    /**
     * 初始化远程模式的BootStrap，本地没有任何Server，所有操作直接请求远程Server。
     * @param servers 远程Server 列表
     * @param properties 配置属性
     */
    public BootStrap(List<URI> servers,StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, Serializer<Q> querySerializer, Serializer<R> resultSerializer, Properties properties) {
        this(null, servers, stateFactory, entrySerializer, querySerializer, resultSerializer, properties);
    }

    /**
     * 初始化本地Server模式BootStrap，本地包含一个Server，模式请求本地Server通信。
     * @param roll 本地Server的角色。
     * @param properties 配置属性
     */
    public BootStrap(JournalKeeperServer.Roll roll, StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, Serializer<Q> querySerializer, Serializer<R> resultSerializer, Properties properties) {
        this(roll, null, stateFactory, entrySerializer, querySerializer, resultSerializer, properties);
    }


    private BootStrap(JournalKeeperServer.Roll roll, List<URI> servers, StateFactory<E, Q, R> stateFactory, Serializer<E> entrySerializer, Serializer<Q> querySerializer, Serializer<R> resultSerializer, Properties properties) {
        this.stateFactory = stateFactory;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
        this.properties = properties;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(SCHEDULE_EXECUTOR_THREADS, new NamedThreadFactory("JournalKeeper-Scheduled-Executor"));
        this.asyncExecutorService = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, new NamedThreadFactory("JournalKeeper-Async-Executor"));
        this.roll = roll;
        this.server = createServer();
        this.clientServerRpcAccessPoint = ServiceSupport.load(RpcAccessPointFactory.class).createClientServerRpcAccessPoint(servers, properties);
    }

    private Server<E, Q, R> createServer() {
        if(null != roll) {
            switch (roll) {
                case VOTER:
                    return new Voter<>(stateFactory,entrySerializer, querySerializer,resultSerializer, scheduledExecutorService, asyncExecutorService, properties);
                case OBSERVER:
                    return new Observer<>(stateFactory,entrySerializer, querySerializer,resultSerializer, scheduledExecutorService, asyncExecutorService, properties);
            }
        }
        return null;
    }

    @Override
    public JournalKeeperClient<E, Q, R> getClient() {
        Client<E, Q, R> client;
        if(this.server == null) {
            client = new Client<>(clientServerRpcAccessPoint, entrySerializer, querySerializer, resultSerializer, properties);
        } else {
            client = new Client<>(new LocalDefaultRpcAccessPoint(server, clientServerRpcAccessPoint), entrySerializer, querySerializer, resultSerializer,  properties);
        }
        return client;
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.asyncExecutorService.shutdown();
    }

    @Override
    public JournalKeeperServer<E, Q, R> getServer() {
        return server;
    }


}
