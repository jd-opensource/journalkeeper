package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.coordinating.network.command.UnWatchRequest;
import com.jd.journalkeeper.coordinating.server.watcher.WatcherHandler;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * UnWatchRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class UnWatchRequestHandler implements CoordinatingCommandHandler {

    private WatcherHandler watcherHandler;

    public UnWatchRequestHandler(WatcherHandler watcherHandler) {
        this.watcherHandler = watcherHandler;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        UnWatchRequest unWatchRequest = (UnWatchRequest) command.getPayload();
        watcherHandler.removeWatcher(unWatchRequest.getKey(), transport);
        return BooleanResponse.build();
    }

    @Override
    public int type() {
        return CoordinatingCommands.UN_WATCH_REQUEST.getType();
    }
}