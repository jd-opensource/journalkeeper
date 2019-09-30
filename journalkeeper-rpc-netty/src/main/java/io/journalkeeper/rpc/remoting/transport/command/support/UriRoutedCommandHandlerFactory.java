package io.journalkeeper.rpc.remoting.transport.command.support;

import io.journalkeeper.rpc.remoting.transport.Transport;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;
import io.journalkeeper.rpc.remoting.transport.command.handler.CommandHandlerFactory;
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
            logger.warn("No handler for uri: {}!", command.getHeader().getDestination());
            return null;
        }
    }
}
