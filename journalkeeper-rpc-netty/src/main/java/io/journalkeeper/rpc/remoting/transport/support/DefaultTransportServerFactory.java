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
import io.journalkeeper.rpc.remoting.event.TransportEvent;
import io.journalkeeper.rpc.remoting.transport.RequestBarrier;
import io.journalkeeper.rpc.remoting.transport.TransportServer;
import io.journalkeeper.rpc.remoting.transport.TransportServerFactory;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
import io.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.filter.CommandHandlerFilterFactory;
import io.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFilterFactory;
import io.journalkeeper.rpc.remoting.transport.command.support.RequestHandler;
import io.journalkeeper.rpc.remoting.transport.command.support.ResponseHandler;
import io.journalkeeper.rpc.remoting.transport.config.ServerConfig;

/**
 * 默认通信服务工厂
 * author: gaohaoxiang
 *
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

    public CommandHandlerFactory getCommandHandlerFactory() {
        return commandHandlerFactory;
    }
}