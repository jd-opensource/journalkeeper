package com.jd.journalkeeper.coordinating.server.network;

import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CoordinatingExceptionHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingExceptionHandler implements ExceptionHandler {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingExceptionHandler.class);

    @Override
    public void handle(Transport transport, Command command, Throwable throwable) {
        logger.error("process command exception, header: {}, payload: {}, transport: {}",
                command.getHeader(), command.getPayload(), transport, throwable);

        transport.stop();
    }
}