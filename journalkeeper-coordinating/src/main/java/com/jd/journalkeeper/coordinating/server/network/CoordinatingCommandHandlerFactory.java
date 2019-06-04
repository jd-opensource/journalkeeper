package com.jd.journalkeeper.coordinating.server.network;

import com.jd.journalkeeper.coordinating.server.CoordinatingContext;
import com.jd.journalkeeper.coordinating.server.handler.CommandHandlerRegistry;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;

import java.util.List;

/**
 * CoordinatingCommandHandlerFactory
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingCommandHandlerFactory extends DefaultCommandHandlerFactory {

    public CoordinatingCommandHandlerFactory(CoordinatingContext context) {
        List<CommandHandler> handlers = new CommandHandlerRegistry(context).getHandlers();
        for (CommandHandler handler : handlers) {
            super.register(handler);
        }
    }
}