package com.jd.journalkeeper.rpc.remoting.transport.support;


import com.jd.journalkeeper.rpc.handler.ExceptionChannelHandler;
import com.jd.journalkeeper.rpc.remoting.concurrent.EventBus;
import com.jd.journalkeeper.rpc.remoting.event.TransportEvent;
import com.jd.journalkeeper.rpc.remoting.event.TransportEventHandler;
import com.jd.journalkeeper.rpc.remoting.transport.RequestBarrier;
import com.jd.journalkeeper.rpc.remoting.transport.TransportServerSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.codec.support.NettyDecoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.support.NettyEncoder;
import com.jd.journalkeeper.rpc.remoting.transport.command.CommandDispatcher;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandDispatcher;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import com.jd.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import com.jd.journalkeeper.rpc.remoting.transport.handler.CommandInvocation;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;

/**
 * 默认通信服务
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/22
 */
public class DefaultTransportServer extends TransportServerSupport {

    private Codec codec;
    private ExceptionHandler exceptionHandler;
    private RequestBarrier requestBarrier;
    private RequestHandler requestHandler;
    private ResponseHandler responseHandler;
    private EventBus<TransportEvent> transportEventBus;

    public DefaultTransportServer(ServerConfig serverConfig, String host, int port, Codec codec, ExceptionHandler exceptionHandler,
                                  RequestBarrier requestBarrier, RequestHandler requestHandler, ResponseHandler responseHandler,
                                  EventBus<TransportEvent> transportEventBus) {
        super(serverConfig, host, port);
        this.codec = codec;
        this.exceptionHandler = exceptionHandler;
        this.requestBarrier = requestBarrier;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.transportEventBus = transportEventBus;
    }

    @Override
    protected ChannelHandler newChannelHandlerPipeline() {
        final CommandDispatcher commandDispatcher = new DefaultCommandDispatcher(requestBarrier, requestHandler, responseHandler);
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline()
                        .addLast(new NettyDecoder(codec))
                        .addLast(new NettyEncoder(codec))
                        .addLast(new TransportEventHandler(requestBarrier, transportEventBus))
                        .addLast(new ExceptionChannelHandler(exceptionHandler, requestBarrier))
                        .addLast(new CommandInvocation(commandDispatcher));
            }
        };
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        transportEventBus.start();
    }
    @Override
    protected void doStop() {
        super.doStop();
        responseHandler.stop();
        transportEventBus.stop(false);
        requestBarrier.clear();
    }
}