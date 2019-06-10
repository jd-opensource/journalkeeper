package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.coordinating.network.command.WatchRequest;
import com.jd.journalkeeper.coordinating.server.watcher.WatcherHandler;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * WatchRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class WatchRequestHandler implements CoordinatingCommandHandler {

    private WatcherHandler watcherHandler;

    public WatchRequestHandler(WatcherHandler watcherHandler) {
        this.watcherHandler = watcherHandler;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        WatchRequest watchRequest = (WatchRequest) command.getPayload();
        watcherHandler.addWatcher(watchRequest.getKey(), transport);
        return BooleanResponse.build();
    }

    @Override
    public int type() {
        return CoordinatingCommands.WATCH_REQUEST.getType();
    }
}