package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.ExistRequest;
import com.jd.journalkeeper.coordinating.network.command.ExistResponse;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * ExistRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class ExistRequestHandler implements CoordinatingCommandHandler {

    private CoordinatingKeeperServer keeperServer;

    public ExistRequestHandler(CoordinatingKeeperServer keeperServer) {
        this.keeperServer = keeperServer;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        ExistRequest existRequest = (ExistRequest) command.getPayload();
        boolean exist = keeperServer.getState().exist(existRequest.getKey());

        ExistResponse existResponse = new ExistResponse();
        existResponse.setExist(exist);
        return new Command(existResponse);
    }

    @Override
    public int type() {
        return CoordinatingCommands.EXIST_REQUEST.getType();
    }
}