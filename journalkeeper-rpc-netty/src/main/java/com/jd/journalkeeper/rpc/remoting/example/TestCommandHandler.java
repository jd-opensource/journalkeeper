package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class TestCommandHandler implements CommandHandler, Type {
    private final TestInterface testImpl;

    public TestCommandHandler(TestInterface testImpl) {
        this.testImpl = testImpl;
    }

    @Override
    public Command handle(Transport transport, Command command) {
        return new Command(
                new TestHeader(0, false, Direction.RESPONSE, command.getHeader().getRequestId(), -1, System.currentTimeMillis(), (short) 0, null),
                new StringPayload(testImpl.hello(((StringPayload )command.getPayload()).getPayload())));
    }

    @Override
    public int type() {
        return 1;
    }
}
