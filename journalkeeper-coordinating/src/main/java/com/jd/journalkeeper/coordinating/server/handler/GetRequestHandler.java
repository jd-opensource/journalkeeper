package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.base.Serializer;
import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import com.jd.journalkeeper.coordinating.network.command.GetRequest;
import com.jd.journalkeeper.coordinating.network.command.GetResponse;
import com.jd.journalkeeper.coordinating.server.CoordinatingCodes;
import com.jd.journalkeeper.coordinating.server.domain.CoordinatingValue;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;

/**
 * GetRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class GetRequestHandler implements CoordinatingCommandHandler {

    private CoordinatingKeeperServer keeperServer;
    private Serializer serializer;

    public GetRequestHandler(CoordinatingKeeperServer keeperServer, Serializer serializer) {
        this.keeperServer = keeperServer;
        this.serializer = serializer;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        GetRequest getRequest = (GetRequest) command.getPayload();
        byte[] valueBytes = keeperServer.getState().get(getRequest.getKey());

        if (valueBytes == null) {
            return BooleanResponse.build(CoordinatingCodes.KEY_NOT_EXIST.getType());
        }

        CoordinatingValue value = (CoordinatingValue) serializer.parse(valueBytes);

        GetResponse response = new GetResponse();
        response.setValue(value.getValue());
        response.setModifyTime(value.getModifyTime());
        response.setCreateTime(value.getCreateTime());
        return new Command(response);
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_REQUEST.getType();
    }
}