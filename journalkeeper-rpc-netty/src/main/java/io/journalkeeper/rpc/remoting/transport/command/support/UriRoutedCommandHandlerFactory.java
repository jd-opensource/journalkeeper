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
package io.journalkeeper.rpc.remoting.transport.command.support;

import io.journalkeeper.rpc.StatusCode;
import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Header;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
import io.journalkeeper.rpc.utils.CommandSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按照URI分流的CommandHandlerFactory
 * @author LiYue
 * Date: 2019/9/30
 */
public class UriRoutedCommandHandlerFactory implements CommandHandlerFactory {
    private static final Logger logger = LoggerFactory.getLogger(UriRoutedCommandHandlerFactory.class);
    private Map<URI, DefaultCommandHandlerFactory> handlerFactoryMap = new ConcurrentHashMap<>();
    private final static CommandHandler defaultHandler = new NoUriCommandHandler();
    @Override
    public CommandHandler getHandler(Command command) {
        URI destination = command.getHeader().getDestination();
        if(null != destination) {
            DefaultCommandHandlerFactory factory = handlerFactoryMap.get(destination);
            if(null != factory) {
//                logger.info("Found request handler of uri: {}.", command.getHeader().getDestination());
                return factory.getHandler(command);
            }
        }
        return defaultHandler;
    }
    public void register(URI destination, CommandHandler commandHandler) {
        DefaultCommandHandlerFactory factory = handlerFactoryMap.computeIfAbsent(destination,  ignored -> new DefaultCommandHandlerFactory());
        factory.register(commandHandler);
    }

    public void unRegister(URI destination) {
        handlerFactoryMap.remove(destination);
    }

    private static class NoUriCommandHandler implements CommandHandler {

        @Override
        public Command handle(Transport transport, Command command) {

            return CommandSupport.newVoidPayloadResponse(
                    StatusCode.SERVER_NOT_FOUND.getCode(),
                    String.format("No server for uri: %s", command.getHeader().getDestination().toString()),
                    command
                    );
        }
    }
}
