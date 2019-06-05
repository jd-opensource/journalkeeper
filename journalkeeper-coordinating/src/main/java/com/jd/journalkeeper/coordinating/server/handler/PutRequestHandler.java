package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PutRequest;
import com.jd.journalkeeper.coordinating.network.command.PutResponse;
import com.jd.journalkeeper.coordinating.server.domain.CoordinatingValue;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * PutRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class PutRequestHandler implements CoordinatingCommandHandler {

    private CoordinatingKeeperServer keeperServer;
    private Serializer serializer;

    public PutRequestHandler(CoordinatingKeeperServer keeperServer, Serializer serializer) {
        this.keeperServer = keeperServer;
        this.serializer = serializer;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        PutRequest putRequest = (PutRequest) command.getPayload();
        byte[] currentValue = keeperServer.getState().get(putRequest.getKey());
        CoordinatingValue value = null;

        if (currentValue != null) {
            value = (CoordinatingValue) serializer.parse(currentValue);
        } else {
            value = new CoordinatingValue();
            value.setCreateTime(System.currentTimeMillis());
        }

        value.setValue(putRequest.getValue());
        value.setModifyTime(System.currentTimeMillis());
        keeperServer.getState().put(putRequest.getKey(), serializer.serialize(value));

        PutResponse response = new PutResponse();
        response.setModifyTime(value.getModifyTime());
        response.setCreateTime(value.getCreateTime());
        return new Command(response);
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_REQUEST.getType();
    }
}