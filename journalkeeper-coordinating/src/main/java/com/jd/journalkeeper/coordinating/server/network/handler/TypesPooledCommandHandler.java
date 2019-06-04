package com.jd.journalkeeper.coordinating.server.network.handler;

import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * PooledCommandHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class TypesPooledCommandHandler extends PooledCommandHandler implements Types {

    private CommandHandler delegate;

    public TypesPooledCommandHandler(CommandHandler delegate, List<ExecutorService> executorServices) {
        super(delegate, executorServices);
        this.delegate = delegate;
    }

    @Override
    public int[] types() {
        return ((Types) delegate).types();
    }
}