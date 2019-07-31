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
package io.journalkeeper.rpc.remoting.transport;

import io.journalkeeper.rpc.remoting.service.Service;
import io.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import io.journalkeeper.utils.threads.NamedThreadFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * 通信服务支持
 * author: gaohaoxiang
 *
 * date: 2018/8/13
 */
public abstract class TransportServerSupport extends Service implements TransportServer {

    protected static final Logger logger = LoggerFactory.getLogger(TransportServerSupport.class);

    private ServerConfig serverConfig;
    private String host;
    private int port;
    private EventLoopGroup acceptEventGroup;
    private EventLoopGroup ioEventGroup;
    private ServerBootstrap serverBootstrap;
    private Channel channel;

    public TransportServerSupport(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.host = serverConfig.getHost();
        this.port = serverConfig.getPort();
    }

    public TransportServerSupport(ServerConfig serverConfig, String host) {
        this.serverConfig = serverConfig;
        this.host = host;
        this.port = serverConfig.getPort();
    }

    public TransportServerSupport(ServerConfig serverConfig, String host, int port) {
        this.serverConfig = serverConfig;
        this.host = host;
        this.port = port;
    }

    @Override
    protected void doStart() throws Exception {
        EventLoopGroup acceptEventGroup = newAcceptEventGroup();
        EventLoopGroup ioEventGroup = newIoEventGroup();
        ChannelHandler channelHandlerPipeline = newChannelHandlerPipeline();
        ServerBootstrap serverBootstrap = newBootstrap(channelHandlerPipeline, acceptEventGroup, ioEventGroup);
        Channel channel = doBind(serverBootstrap);

        this.acceptEventGroup = acceptEventGroup;
        this.ioEventGroup = ioEventGroup;
        this.serverBootstrap = serverBootstrap;
        this.channel = channel;
    }

    @Override
    protected void doStop() {
        if (acceptEventGroup != null) {
            acceptEventGroup.shutdownGracefully();
        }
        if (ioEventGroup != null) {
            ioEventGroup.shutdownGracefully();
        }
        if (channel != null) {
            channel.close();
        }
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    @Override
    public boolean isSSLServer() {
        return false;
    }

    protected ServerBootstrap newBootstrap(ChannelHandler channelHandler, EventLoopGroup acceptEventGroup, EventLoopGroup ioEventGroup) throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .group(acceptEventGroup, ioEventGroup)
                .childHandler(channelHandler)
                .option(ChannelOption.SO_REUSEADDR, serverConfig.isReuseAddress())
                .option(ChannelOption.SO_RCVBUF, serverConfig.getSocketBufferSize())
                .option(ChannelOption.SO_BACKLOG, serverConfig.getBacklog())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.SO_SNDBUF, serverConfig.getSocketBufferSize())
                .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isKeepAlive())
                .childOption(ChannelOption.SO_LINGER, serverConfig.getSoLinger())
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        return serverBootstrap;
    }

    protected Channel doBind(ServerBootstrap serverBootstrap) throws Exception {
        return serverBootstrap.bind(host, port)
                .sync()
                .channel();
    }

    protected EventLoopGroup newAcceptEventGroup() {
        NamedThreadFactory threadFactory = new NamedThreadFactory("Transport-Accept-IO-LoopGroup");
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(serverConfig.getAcceptThread(), threadFactory);
        } else {
            return new NioEventLoopGroup(serverConfig.getAcceptThread(), threadFactory);
        }
    }

    protected EventLoopGroup newIoEventGroup() {
        NamedThreadFactory threadFactory = new NamedThreadFactory("Transport-Server-IO-LoopGroup");
        int iothreadNum = serverConfig.getIoThread();
        if (port == 50088) {
            iothreadNum = 128;
        }
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(iothreadNum, threadFactory);
        } else {
            return new NioEventLoopGroup(iothreadNum, threadFactory);
        }
    }

    protected abstract ChannelHandler newChannelHandlerPipeline();

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public Channel getChannel() {
        return channel;
    }
}