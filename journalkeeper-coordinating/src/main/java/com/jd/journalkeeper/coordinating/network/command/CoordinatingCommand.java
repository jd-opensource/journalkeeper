package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.codec.CoordinatingHeader;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;

/**
 * CoordinatingCommand
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingCommand extends Command {

    public CoordinatingCommand(CoordinatingPayload payload) {
        this(payload.type(), payload);
    }

    public CoordinatingCommand(int type, CoordinatingPayload payload) {
        setHeader(new CoordinatingHeader(Direction.REQUEST, type));
        setPayload(payload);
    }

    @Override
    public String toString() {
        return header.toString();
    }
}