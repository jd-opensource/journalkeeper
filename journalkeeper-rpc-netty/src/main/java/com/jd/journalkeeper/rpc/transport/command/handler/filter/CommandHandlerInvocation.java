package com.jd.journalkeeper.rpc.transport.command.handler.filter;

import com.jd.journalkeeper.rpc.transport.Transport;
import com.jd.journalkeeper.rpc.transport.command.Command;
import com.jd.journalkeeper.rpc.transport.command.handler.CommandHandler;
import com.jd.journalkeeper.rpc.transport.exception.TransportException;

import java.util.Iterator;
import java.util.List;

/**
 * 命令处理器上下文
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/16
 */
public class CommandHandlerInvocation {

    private Transport transport;
    private Command command;
    private CommandHandler commandHandler;
    private Iterator<CommandHandlerFilter> filterIterator;
    private CommandHandlerContext context;

    public CommandHandlerInvocation(Transport transport, Command command, CommandHandler commandHandler, List<CommandHandlerFilter> filterList) {
        this.transport = transport;
        this.command = command;
        this.commandHandler = commandHandler;
        this.filterIterator = (filterList == null || filterList.isEmpty()) ? null : filterList.iterator();
    }

    public Command invoke() throws TransportException {
        if (filterIterator == null || !filterIterator.hasNext()) {
            return commandHandler.handle(transport, command);
        } else {
            return filterIterator.next().invoke(this);
        }
    }

    public Transport getTransport() {
        return transport;
    }

    public Command getCommand() {
        return command;
    }

    public CommandHandlerContext getContext() {
        if (context == null) {
            context = new CommandHandlerContext();
        }
        return context;
    }
}