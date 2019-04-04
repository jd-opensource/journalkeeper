package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.TransportClient;
import com.jd.journalkeeper.rpc.remoting.transport.config.ClientConfig;
import com.jd.journalkeeper.rpc.remoting.transport.support.DefaultTransportClientFactory;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class Client {
    public static void main(String [] args) throws Exception {
        DefaultTransportClientFactory defaultTransportClientFactory = new DefaultTransportClientFactory(new TestCodec());
        TransportClient transportClient = defaultTransportClientFactory.create(new ClientConfig());
        transportClient.start();
        Transport transport = transportClient.createTransport("localhost:9999");

        TestInterface testTpc = new TestInterfaceStub(transport);
        System.out.println("Response from server: " + testTpc.hello("JournalKeeper"));
        transport.stop();
        transportClient.stop();
    }
}
