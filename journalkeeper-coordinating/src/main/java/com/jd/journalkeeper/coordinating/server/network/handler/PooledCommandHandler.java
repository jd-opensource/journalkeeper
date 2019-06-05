package com.jd.journalkeeper.coordinating.server.network.handler;

import com.jd.journalkeeper.coordinating.network.command.KeyPayload;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.provider.ExecutorServiceProvider;
import com.sun.org.apache.xpath.internal.ExtensionsProvider;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * PooledCommandHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PooledCommandHandler implements CommandHandler, ExecutorServiceProvider {

    private CommandHandler delegate;
    private List<ExecutorService> executorServices;

    public PooledCommandHandler(CommandHandler delegate, List<ExecutorService> executorServices) {
        this.delegate = delegate;
        this.executorServices = executorServices;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        return delegate.handle(transport, command);
    }

    @Override
    public ExecutorService getExecutorService(Transport transport, Command command) {
        if (delegate instanceof ExtensionsProvider) {
            return ((ExecutorServiceProvider) delegate).getExecutorService(transport, command);
        }

        Object payload = command.getPayload();
        int hashCode = 0;

        if (payload instanceof KeyPayload) {
            KeyPayload keyPayload = (KeyPayload) payload;
            hashCode = ArrayUtils.hashCode(keyPayload.getKey());
        } else {
            hashCode = transport.toString().hashCode();
        }

        return executorServices.get(Math.abs(hashCode) % executorServices.size());
    }
}