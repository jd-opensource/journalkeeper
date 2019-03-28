package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class TestCommandHandler implements CommandHandler {
    private final TestInterface testImpl;

    public TestCommandHandler(TestInterface testImpl) {
        this.testImpl = testImpl;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        if(command.getHeader().getType() == 1) {
            return new Command(
                    new TestHeader(0, false, Direction.RESPONSE, command.getHeader().getRequestId(), -1, System.currentTimeMillis(), (short) 0, null),
                    new StringPayload(testImpl.hello(((StringPayload )command.getPayload()).getPayload())));
        } else {
            return new Command(
                    new TestHeader(0, false, Direction.RESPONSE, command.getHeader().getRequestId(), -1, System.currentTimeMillis(), (short) -1,"命令类型不对"),
                    null);
        }
    }
}
