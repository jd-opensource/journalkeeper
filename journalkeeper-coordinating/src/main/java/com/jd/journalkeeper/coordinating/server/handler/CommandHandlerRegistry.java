package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.server.CoordinatingContext;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfigs;
import com.jd.journalkeeper.coordinating.server.network.handler.TypePooledCommandHandler;
import com.jd.journalkeeper.coordinating.server.network.handler.TypesPooledCommandHandler;
import com.jd.journalkeeper.coordinating.server.util.PropertiesUtils;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.utils.threads.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * CommandHandlerRegistry
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CommandHandlerRegistry {

    private List<CommandHandler> handlers = new ArrayList<>();
    private List<ExecutorService> executorServices;

    public CommandHandlerRegistry(CoordinatingContext context) {
        this.executorServices = createExecutorServices(context);
        this.register(new HeartbeatRequestHandler());
        this.register(new GetClusterRequestHandler(context.getConfig(), context.getKeeperServer()));
        this.register(new PutRequestHandler(context.getKeeperServer(), context.getWatcherHandler(), context.getSerializer()));
        this.register(new GetRequestHandler(context.getKeeperServer(), context.getSerializer()));
        this.register(new RemoveRequestHandler(context.getKeeperServer(), context.getWatcherHandler()));
        this.register(new ExistRequestHandler(context.getKeeperServer()));
        this.register(new CompareAndSetRequestHandler(context.getKeeperServer(), context.getWatcherHandler(), context.getSerializer()));
        this.register(new WatchRequestHandler(context.getWatcherHandler()));
        this.register(new UnWatchRequestHandler(context.getWatcherHandler()));
    }

    protected List<ExecutorService> createExecutorServices(CoordinatingContext context) {
        int threads = PropertiesUtils.getProperty(context.getConfig().getProperties(), CoordinatingConfigs.SERVER_EXECUTOR_THREADS, CoordinatingConfigs.DEFAULT_SERVER_EXECUTOR_THREADS);
        int queueSize = PropertiesUtils.getProperty(context.getConfig().getProperties(), CoordinatingConfigs.SERVER_EXECUTOR_QUEUE_SIZE, CoordinatingConfigs.DEFAULT_SERVER_EXECUTOR_QUEUE_SIZE);
        List<ExecutorService> result = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(queueSize),
                    new NamedThreadFactory("coordinating-executor" + i));
            result.add(executor);
        }
        return result;
    }

    protected void register(CommandHandler handler) {
        handlers.add(wrap(handler));
    }

    protected CommandHandler wrap(CommandHandler handler) {
        if (handler instanceof Type) {
            return new TypePooledCommandHandler(handler, executorServices);
        } else if (handler instanceof Types) {
            return new TypesPooledCommandHandler(handler, executorServices);
        } else {
            return handler;
        }
    }

    public List<CommandHandler> getHandlers() {
        return handlers;
    }

}