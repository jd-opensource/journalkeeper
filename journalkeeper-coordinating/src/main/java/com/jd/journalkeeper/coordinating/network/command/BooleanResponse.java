package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.codec.CoordinatingHeader;
import com.jd.journalkeeper.coordinating.server.CoordinatingCodes;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;

/**
 * BooleanResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class BooleanResponse implements CoordinatingPayload {

    public static Command build() {
        return build(CoordinatingCodes.SUCCESS.getType());
    }

    public static Command build(int code) {
        return build(code, null);
    }

    public static Command build(int code, String message) {
        CoordinatingHeader header = new CoordinatingHeader(Direction.RESPONSE, CoordinatingCommands.BOOLEAN_RESPONSE.getType());
        header.setStatus((short) code);
        header.setError(message);
        return new Command(header, null);
    }

    @Override
    public int type() {
        return CoordinatingCommands.BOOLEAN_RESPONSE.getType();
    }
}