package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetRequest;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetResponse;
import com.jd.journalkeeper.coordinating.server.domain.CoordinatingValue;
import com.jd.journalkeeper.coordinating.server.watcher.WatcherHandler;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

import java.util.Objects;

/**
 * CompareAndSetRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CompareAndSetRequestHandler implements CoordinatingCommandHandler {

    private CoordinatingKeeperServer keeperServer;
    private WatcherHandler watcherHandler;
    private Serializer serializer;

    public CompareAndSetRequestHandler(CoordinatingKeeperServer keeperServer, WatcherHandler watcherHandler, Serializer serializer) {
        this.keeperServer = keeperServer;
        this.watcherHandler = watcherHandler;
        this.serializer = serializer;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        CompareAndSetRequest compareAndSetRequest = (CompareAndSetRequest) command.getPayload();
        byte[] valueBytes = keeperServer.getState().get(compareAndSetRequest.getKey());
        CoordinatingValue value = null;

        if (valueBytes != null) {
            value = (CoordinatingValue) serializer.parse(valueBytes);
            if (!Objects.deepEquals(value.getValue(), compareAndSetRequest.getExpect())) {
                return new Command(new CompareAndSetResponse(false, value.getValue()));
            }
        }

        if (value == null) {
            value = new CoordinatingValue();
            value.setCreateTime(System.currentTimeMillis());
        }

        value.setValue(compareAndSetRequest.getUpdate());
        value.setModifyTime(System.currentTimeMillis());
        keeperServer.getState().put(compareAndSetRequest.getKey(), serializer.serialize(value));
        watcherHandler.notifyKeyChanged(compareAndSetRequest.getKey(), value);

        CompareAndSetResponse response = new CompareAndSetResponse(true, value.getValue());
        return new Command(response);
    }

    @Override
    public int type() {
        return CoordinatingCommands.COMPARE_AND_SET_REQUEST.getType();
    }
}