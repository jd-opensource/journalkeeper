package com.jd.journalkeeper.rpc.transport.command.support;

import com.jd.journalkeeper.rpc.protocol.ExceptionHandlerProvider;
import com.jd.journalkeeper.rpc.protocol.Protocol;
import com.jd.journalkeeper.rpc.transport.RequestBarrier;
import com.jd.journalkeeper.rpc.transport.command.CommandDispatcher;
import com.jd.journalkeeper.rpc.transport.command.CommandDispatcherFactory;
import com.jd.journalkeeper.rpc.transport.command.handler.CommandHandlerFactory;
import com.jd.journalkeeper.rpc.transport.command.handler.ExceptionHandler;
import com.jd.journalkeeper.rpc.transport.command.handler.filter.CommandHandlerFilterFactory;
import com.jd.journalkeeper.rpc.transport.config.TransportConfig;

/**
 * 默认命令调度器工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/16
 */
public class DefaultCommandDispatcherFactory implements CommandDispatcherFactory {

    private TransportConfig transportConfig;
    private RequestBarrier requestBarrier;
    private CommandHandlerFilterFactory commandHandlerFilterFactory;
    private ExceptionHandler exceptionHandler;

    public DefaultCommandDispatcherFactory(TransportConfig transportConfig, RequestBarrier requestBarrier, CommandHandlerFilterFactory commandHandlerFilterFactory, ExceptionHandler exceptionHandler) {
        this.transportConfig = transportConfig;
        this.requestBarrier = requestBarrier;
        this.commandHandlerFilterFactory = commandHandlerFilterFactory;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public CommandDispatcher getCommandDispatcher(Protocol protocol) {
        ExceptionHandler exceptionHandler = getExceptionHandler(protocol);
        CommandHandlerFactory commandHandlerFactory = protocol.createCommandHandlerFactory();
        RequestHandler requestHandler = new RequestHandler(commandHandlerFactory, commandHandlerFilterFactory, exceptionHandler);
        ResponseHandler responseHandler = new ResponseHandler(transportConfig, requestBarrier, exceptionHandler);
        return new DefaultCommandDispatcher(requestBarrier, requestHandler, responseHandler);
    }

    protected ExceptionHandler getExceptionHandler(Protocol protocol) {
        if (protocol instanceof ExceptionHandlerProvider) {
            return ((ExceptionHandlerProvider) protocol).getExceptionHandler();
        } else {
            return this.exceptionHandler;
        }
    }
}