package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * HeartbeatRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class HeartbeatRequestHandler implements CoordinatingCommandHandler {

    @Override
    public Command handle(Transport transport, Command command) {
        return BooleanResponse.build();
    }

    @Override
    public int type() {
        return CoordinatingCommands.HEARTBEAT_REQUEST.getType();
    }
}