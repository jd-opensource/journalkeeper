package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.coordinating.network.command.RemoveRequest;
import com.jd.journalkeeper.coordinating.server.CoordinatingCodes;
import com.jd.journalkeeper.coordinating.server.watcher.WatcherHandler;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * RemoveRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class RemoveRequestHandler implements CoordinatingCommandHandler {

    private CoordinatingKeeperServer keeperServer;
    private WatcherHandler watcherHandler;

    public RemoveRequestHandler(CoordinatingKeeperServer keeperServer, WatcherHandler watcherHandler) {
        this.keeperServer = keeperServer;
        this.watcherHandler = watcherHandler;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        RemoveRequest removeRequest = (RemoveRequest) command.getPayload();
        boolean exist = keeperServer.getState().exist(removeRequest.getKey());

        if (!exist) {
            return BooleanResponse.build(CoordinatingCodes.KEY_NOT_EXIST.getType());
        }

        keeperServer.getState().remove(removeRequest.getKey());
        watcherHandler.notifyKeyRemoved(removeRequest.getKey());
        return BooleanResponse.build(CoordinatingCodes.SUCCESS.getType());
    }

    @Override
    public int type() {
        return CoordinatingCommands.REMOVE_REQUEST.getType();
    }
}