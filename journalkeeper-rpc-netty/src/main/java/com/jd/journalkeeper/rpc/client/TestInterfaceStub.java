package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.transport.Transport;
import com.jd.journalkeeper.rpc.transport.command.Command;
import com.jd.journalkeeper.rpc.transport.command.Direction;
import com.jd.journalkeeper.rpc.transport.command.Header;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class TestInterfaceStub implements TestInterface {

    private AtomicInteger requestId = new AtomicInteger(0);
    private final Transport transport;

    public TestInterfaceStub(Transport transport) {
        this.transport = transport;
    }

    @Override
    public String hello(String name) {
        Header header = new TestHeader();
        header.setOneWay(false);
        header.setDirection(Direction.REQUEST);
        header.setVersion(0);
        header.setType(1);
        header.setRequestId(requestId.getAndIncrement());

        Command command = new Command(header,new StringPayload(name));
        Command resp = transport.sync(command);
        return ((StringPayload) resp.getPayload()).getPayload();

    }
}
