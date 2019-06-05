package com.jd.journalkeeper.rpc.remoting.transport.support;

import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.transport.RequestBarrier;
import com.jd.journalkeeper.rpc.remoting.transport.TransportServer;
import com.jd.journalkeeper.rpc.remoting.transport.TransportServerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilterFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFilterFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;

/**
 * 默认通信服务工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/22
 */
public class DefaultTransportServerFactory implements TransportServerFactory {

    private Codec codec;
    private CommandHandlerFactory commandHandlerFactory;
    private ExceptionHandler exceptionHandler;
    private EventBus<TransportEvent> eventBus;

    public DefaultTransportServerFactory(Codec codec, CommandHandlerFactory commandHandlerFactory) {
        this(codec, commandHandlerFactory, null);
    }

    public DefaultTransportServerFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler) {
        this(codec, commandHandlerFactory, exceptionHandler, new EventBus<>("DefaultTransportServerEventBus"));
    }

    public DefaultTransportServerFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler, EventBus<TransportEvent> eventBus) {
        this.codec = codec;
        this.commandHandlerFactory = commandHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.eventBus = eventBus;
    }

    @Override
    public TransportServer bind(ServerConfig serverConfig) {
        return bind(serverConfig, serverConfig.getHost(), serverConfig.getPort());
    }

    @Override
    public TransportServer bind(ServerConfig serverConfig, String host) {
        return bind(serverConfig, host, serverConfig.getPort());
    }

    @Override
    public TransportServer bind(ServerConfig serverConfig, String host, int port) {
        CommandHandlerFilterFactory commandHandlerFilterFactory = new DefaultCommandHandlerFilterFactory();
        RequestBarrier requestBarrier = new RequestBarrier(serverConfig);
        RequestHandler requestHandler = new RequestHandler(commandHandlerFactory, commandHandlerFilterFactory, exceptionHandler);
        ResponseHandler responseHandler = new ResponseHandler(serverConfig, requestBarrier, exceptionHandler);
        return new DefaultTransportServer(serverConfig, host, port, codec, exceptionHandler, requestBarrier, requestHandler, responseHandler, eventBus);
    }
}