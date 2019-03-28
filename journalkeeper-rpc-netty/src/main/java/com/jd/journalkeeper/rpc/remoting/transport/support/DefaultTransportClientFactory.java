package com.jd.journalkeeper.rpc.remoting.transport.support;

import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.transport.RequestBarrier;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClientFactory;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilterFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFilterFactory;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import com.jd.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;

/**
 * 默认通信客户端工厂
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/24
 */
public class  DefaultTransportClientFactory implements TransportClientFactory {

    private Codec codec;
    private CommandHandlerFactory commandHandlerFactory;
    private ExceptionHandler exceptionHandler;
    private EventBus<TransportEvent> transportEventBus;

    public DefaultTransportClientFactory(Codec codec) {
        this(codec, (CommandHandlerFactory) null);
    }

    public DefaultTransportClientFactory(Codec codec, EventBus<TransportEvent> transportEventBus) {
        this(codec, null, null, transportEventBus);
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory) {
        this(codec, commandHandlerFactory, null);
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler) {
        this(codec, commandHandlerFactory, exceptionHandler, new EventBus());
    }

    public DefaultTransportClientFactory(Codec codec, CommandHandlerFactory commandHandlerFactory, ExceptionHandler exceptionHandler, EventBus<TransportEvent> transportEventBus) {
        this.codec = codec;
        this.commandHandlerFactory = commandHandlerFactory;
        this.exceptionHandler = exceptionHandler;
        this.transportEventBus = transportEventBus;
    }

    @Override
    public TransportClient create(ClientConfig config) {
        CommandHandlerFilterFactory commandHandlerFilterFactory = new DefaultCommandHandlerFilterFactory();
        RequestBarrier requestBarrier = new RequestBarrier(config);
        RequestHandler requestHandler = new RequestHandler(commandHandlerFactory, commandHandlerFilterFactory, exceptionHandler);
        ResponseHandler responseHandler = new ResponseHandler(config, requestBarrier, exceptionHandler);
        DefaultTransportClient transportClient = new DefaultTransportClient(config, codec, requestBarrier, requestHandler, responseHandler, transportEventBus);
        return new FailoverTransportClient(transportClient, config, transportEventBus);
    }
}