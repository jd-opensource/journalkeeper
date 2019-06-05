package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * WatchRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class WatchRequestHandler implements CoordinatingCommandHandler {

    @Override
    public Command handle(Transport transport, Command command) {
        return null;
    }

    @Override
    public int type() {
        return CoordinatingCommands.WATCH_REQUEST.getType();
    }
}