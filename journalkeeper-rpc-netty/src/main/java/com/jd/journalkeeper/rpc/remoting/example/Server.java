package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import com.jd.journalkeeper.rpc.remoting.transport.config.ServerConfig;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportServerFactory;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class Server {
    public static void main(String [] args) throws Exception {
        DefaultCommandHandlerFactory handlerFactory = new DefaultCommandHandlerFactory();
        handlerFactory.register(new TestCommandHandler(new TestInterfaceImpl()));
        DefaultTransportServerFactory defaultTransportServerFactory = new DefaultTransportServerFactory(
                new TestCodec(), handlerFactory
               );
        defaultTransportServerFactory.bind(new ServerConfig(), "localhost", 9999).start();

    }
}
