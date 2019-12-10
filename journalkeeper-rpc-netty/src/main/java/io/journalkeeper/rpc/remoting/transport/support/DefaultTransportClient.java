/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.transport.support;

import io.journalkeeper.rpc.remoting.concurrent.EventBus;
import io.journalkeeper.rpc.remoting.concurrent.EventListener;
import io.journalkeeper.rpc.remoting.event.TransportEvent;
import io.journalkeeper.rpc.remoting.event.TransportEventHandler;
import io.journalkeeper.rpc.remoting.handler.ClientConnectionHandler;
import io.journalkeeper.rpc.remoting.transport.RequestBarrier;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.TransportClient;
import io.journalkeeper.rpc.remoting.transport.TransportClientSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.codec.support.NettyDecoder;
import io.journalkeeper.rpc.remoting.transport.codec.support.NettyEncoder;
import io.journalkeeper.rpc.remoting.transport.command.CommandDispatcher;
import io.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandDispatcher;
import io.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import io.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import io.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.journalkeeper.rpc.remoting.transport.handler.CommandInvocation;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;

import java.net.SocketAddress;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 默认通信客户端
 * author: gaohaoxiang
 *
 * date: 2018/8/24
 */
public class DefaultTransportClient extends TransportClientSupport implements TransportClient {

    private Codec codec;
    private RequestBarrier requestBarrier;
    private RequestHandler requestHandler;
    private ResponseHandler responseHandler;
    private EventBus<TransportEvent> transportEventBus;
    private Timer clearTimer;

    public DefaultTransportClient(ClientConfig config, Codec codec, final RequestBarrier requestBarrier, RequestHandler requestHandler, ResponseHandler responseHandler, EventBus<TransportEvent> transportEventBus) {
        super(config);
        this.codec = codec;
        this.requestBarrier = requestBarrier;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.transportEventBus = transportEventBus;
        this.clearTimer = new Timer("DefaultTransportClient-Clear-Timer");


    }

    @Override
    protected ChannelHandler newChannelHandlerPipeline() {
        final CommandDispatcher commandDispatcher = new DefaultCommandDispatcher(requestBarrier, requestHandler, responseHandler);
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel channel) {
                channel.pipeline()
                        .addLast(new NettyDecoder(codec))
                        .addLast(new NettyEncoder(codec))
                        .addLast(new ClientConnectionHandler())
                        .addLast(new TransportEventHandler(requestBarrier, transportEventBus))
                        .addLast(new CommandInvocation(commandDispatcher));
            }
        };
    }

    @Override
    public Transport createTransport(String address) throws TransportException {
        return this.createTransport(address, -1);
    }

    @Override
    public Transport createTransport(String address, long connectionTimeout) throws TransportException {
        return this.createTransport(createInetSocketAddress(address), connectionTimeout);
    }

    @Override
    public Transport createTransport(SocketAddress address) throws TransportException {
        return this.createTransport(address, -1);
    }

    @Override
    public Transport createTransport(SocketAddress address, long connectionTimeout) throws TransportException {
        Channel channel = createChannel(address, connectionTimeout);
        return new DefaultChannelTransport(channel, requestBarrier, address);
    }

    @Override
    public void addListener(EventListener<TransportEvent> listener) {
        this.transportEventBus.addListener(listener);
    }

    @Override
    public void removeListener(EventListener<TransportEvent> listener) {
        this.transportEventBus.removeListener(listener);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.clearTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                requestBarrier.evict();
            }
        }, getConfig().getSendTimeout(), getConfig().getSendTimeout());
        transportEventBus.start();
    }

    @Override
    protected void doStop() {
        super.doStop();
        clearTimer.cancel();
        transportEventBus.stop(false);
        requestBarrier.clear();
        responseHandler.stop();
    }
}